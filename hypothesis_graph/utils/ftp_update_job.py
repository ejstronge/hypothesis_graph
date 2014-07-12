
"""
ftp_update_job.py

Script for retrieving Medline records from the NLM FTP server.  Run
this as a cron job.

NOTE: This script requires that the file ~/.netrc exist and contain
only an entry for the NLM public server. See `man 5 netrc` for details
on the netrc file format.
"""
from collections import namedtuple
import hashlib
import ftplib
import netrc
from os import path
import re
import StringIO
import sqlite3
import time


NLM_NETRC = netrc.netrc(file=path.expanduser('~/.netrc'))

FTPConnectionParams = namedtuple(
    'FTPConnectionParams', 'host user account password')

FTPFileParams = namedtuple(
    'FTPFileParams', 'modification_date size unique_file_id filename')

# example - medline14n0746.xml.gz.md5
MEDLINE_ARCHIVE_PATTERN = re.compile('medline\d{2}n\d{4}')

DOWNLOADED_FILES_SCHEMA =\
    """CREATE TABLE nlm_archives (
           id INTEGER PRIMARY KEY,
           size INTEGER NOT NULL,
           record_name TEXT NOT NULL,
           unique_file_id TEXT NOT NULL,
           modification_date TEXT NOT NULL,
           observed_md5 TEXT NOT NULL,
           md5_verified INTEGER NOT NULL,
           download_date TEXT NOT NULL,
           download_location TEXT NOT NULL,
           transferred_for_output INTEGER NOT NULL,  -- Was this record made ready for rsync download?
           downloaded_by_application INTEGER NOT NULL  -- Was this record downloaded?
       );

       CREATE TABLE md5_checksums (
           id INTEGER PRIMARY KEY,
           referenced_record TEXT NOT NULL,  -- Which archive is this checksum for?
           unique_file_id TEXT NOT NULL,
           md5_value TEXT NOT NULL,
           download_date TEXT NOT NULL
       );

       /* archive_notes

       NLM lists retracted papers and other miscellany in text or
       html files with the same basename as the relevant archive (
       e.g., medline14n01.xml and medline14n01.xml.notes.txt)
       */
       CREATE TABLE archive_notes (
           id INTEGER PRIMARY KEY,
           filename TEXT NOT NULL,
           referenced_record TEXT,  -- Which archive is this note for?
           unique_file_id TEXT NOT NULL,
           note TEXT NOT NULL,  -- Text from the note
           download_date TEXT NOT NULL
       );
    """

NEW_ARCHIVE_SQL =\
    """INSERT INTO nlm_archives (
            size, record_name, unique_file_id, modification_date,
            observed_md5, md5_verified, download_date, download_location,
            transferred_for_output, downloaded_by_application) VALUES (
                :size, :referenced_record, :unique_file_id, :modification_date,
                :md5, :download_date, :download_location,
                :transferred_for_output, :downloaded_by_application);
    """

NEW_HASH_SQL =\
    """INSERT INTO md5_checksums (
            referenced_record, unique_file_id, md5_value,
            download_date) VALUES (
                :referenced_record, :unique_file_id, :md5_value,
                :download_date);
    """

NEW_NOTE_SQL =\
    """INSERT INTO archive_notes (
            filename, referenced_record, unique_file_id, note,
            download_date) VALUES (
                :filename, :referenced_record, :unique_file_id, :note,
                :download_date);
    """


# modify=20131125174213;perm=adfr;size=24847843;type=file;unique=4600001UE9FE;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0745.xml.gz
# modify=20131125174556;perm=adfr;size=63;type=file;unique=4600001UEA02;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0002.xml.gz.md5
def parse_mlsd(line):
    """
    Parses lines of text from an FTP directory listing (see examples above)
    """
    metadata_string, filename = line.split()
    metadata = {}
    for param in metadata_string.split(';'):
        if not param:
            continue
        k, v = param.split('=')
        metadata[k] = v
    if metadata['type'] != 'file':
        return
    return FTPFileParams(metadata['modify'], metadata['size'],
                         metadata['unique'], filename)


def get_file_listing(connection, server_dir):
    """
    Returns tuples of file information for each file in server_dir.
    Directories are excluded from this listing
    """
    file_listing = StringIO.StringIO()
    # This should definitely be line-wise but ftplib complains when the NLM
    # server changes mode from BINARY to ASCII and back again. Using StringIO
    # is a workaround.
    connection.retrbinary('MLSD %s' % server_dir, file_listing.write)
    file_listing.seek(0)
    listing = [parse_mlsd(l) for l in file_listing.readlines()]
    return [l for l in listing if l is not None]


def initialize_database_connection(db_file):
    """
    Return a connection to `db_file`. If the file does not exist
    it is created and intialized with `DOWNLOADED_FILES_SCHEMA`.
    """
    if not path.exists(db_file):
        db_con = sqlite3.connect(db_file)
        db_con.executescript(DOWNLOADED_FILES_SCHEMA)
        db_con.commit()
    else:
        db_con = sqlite3.connect(db_file)
    db_con.text_factory = str
    db_con.row_factory = sqlite3.Row
    return db_con


def retrieve_files(connection, server_dir, output_dir, download_database):
    """
    Download new files from `server_dir` to `output_dir`.
    """
    connection.cwd(server_dir)

    db_con = initialize_database_connection(download_database)

    downloaded_files = {row['unique_file_id'] for row in db_con.execute(
        "SELECT unique_file_id FROM nlm_archives")}
    downloaded_files.union({row['unique_file_id'] for row in db_con.execute(
        "SELECT unique_file_id FROM md5_checksums")})
    downloaded_files.union({row['unique_file_id'] for row in db_con.execute(
        "SELECT unique_file_id FROM archive_notes")})

    # XXX TODO remove artificial download limit
    for file_info in get_file_listing(connection, server_dir)[:3]:
        # Don't redownload files. Also, don't download archive
        # statistics as we can look these up online.
        if (file_info.unique_file_id in downloaded_files) or (
                file_info.filename.endswith('stats.html')) or (
                file_info.filename.endswith('.dat')):
            continue

        output_path = path.join(output_dir, file_info.filename)
        with open(output_path, 'wb+') as new_file:
            connection.retrbinary(
                'RETR %s' % file_info.filename, new_file.write)
            new_file.seek(0)
            observed_md5 = hashlib.md5()
            observed_md5.update(new_file.read())

        file_info_dict = file_info._asdict()
        file_info_dict['download_date'] = time.strftime('%Y%m%d%H%M%S')

        # This could be None - would need to examine such files
        referenced_record = MEDLINE_ARCHIVE_PATTERN.match(file_info.filename)
        if referenced_record is not None:
            file_info_dict['referenced_record'] = referenced_record.group(0)
        else:
            file_info_dict['referenced_record'] = None

        if file_info.filename.endswith('.xml.gz'):
            file_info_dict.update(
                observed_md5=observed_md5.digest(), md5_verified=0,
                download_location=output_dir, transferred_for_output=0,
                downloaded_by_application=0)
            db_con.execute(NEW_ARCHIVE_SQL, file_info_dict)

        elif file_info.filename.endswith('.xml.gz.md5'):
            with open(output_path) as hash_file:
                file_info_dict.update(md5_value=hash_file.read())
                db_con.execute(NEW_HASH_SQL, file_info_dict)
        else:
            with open(output_path) as note_file:
                file_info_dict.update(
                    filename=file_info.filename, note=note_file.read())
                db_con.execute(NEW_NOTE_SQL, file_info_dict)
    db_con.commit()
    db_con.close()


if __name__ == '__main__':
    assert len(NLM_NETRC.hosts.keys()
               ) == 1, "The netrc file should contain only one record"
    for server, params in NLM_NETRC.hosts.items():
        FTP_PARAMS = FTPConnectionParams(*([server] + list(params)))
    FTP_CONNECTION = ftplib.FTP(
        host=FTP_PARAMS.host, user=FTP_PARAMS.user, passwd=FTP_PARAMS.password)
