# -*- coding: utf-8 -*-
"""
   ftp_update_job.py
   =================

   Script for retrieving Medline records from the NLM FTP server.  Run
   this as a cron job.

   NOTE: This script requires that a netrc file exist and contain
   only an entry for the NLM public server. See `man 5 netrc` for details
   on the netrc file format.

   (c) 2014, Edward J. Stronge
   Available under the GPLv3 - see LICENSE for details.
"""
import argparse
from collections import namedtuple
import hashlib
import ftplib
import netrc
from os import path
import re
import StringIO
import sqlite3
import time


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
           filename TEXT NOT NULL,
           unique_file_id TEXT NOT NULL UNIQUE,
           modification_date TEXT NOT NULL,
           observed_md5 TEXT NOT NULL,
           md5_verified INTEGER NOT NULL,
           download_date TEXT NOT NULL,
           download_location TEXT NOT NULL,
           transferred_for_output INTEGER NOT NULL,  -- Was this archive made ready for rsync download?
           downloaded_by_application INTEGER NOT NULL  -- Was this archive downloaded?
       );

       CREATE TABLE md5_checksums (
           id INTEGER PRIMARY KEY,
           referenced_record TEXT NOT NULL,  -- Refers to nlm_archives
           unique_file_id TEXT NOT NULL UNIQUE,
           md5_value TEXT NOT NULL,
           download_date TEXT NOT NULL,
           filename TEXT NOT NULL,
           checksum_file_deleted INTEGER NOT NULL
       );

       /* archive_notes

       NLM lists retracted papers and other miscellany in text or
       html files with the same basename as the relevant archive (
       e.g., medline14n01.xml and medline14n01.xml.notes.txt)
       */
       CREATE TABLE archive_notes (
           id INTEGER PRIMARY KEY,
           filename TEXT NOT NULL,
           referenced_record TEXT,  -- Refers nlm_archives
           unique_file_id TEXT NOT NULL UNIQUE,
           download_date TEXT NOT NULL
       );
    """

NEW_ARCHIVE_SQL =\
    """INSERT INTO nlm_archives (
            size, record_name, filename, unique_file_id, modification_date,
            observed_md5, md5_verified, download_date, download_location,
            transferred_for_output, downloaded_by_application
            ) VALUES (
                :size, :referenced_record, :filename, :unique_file_id,
                :modification_date, :observed_md5, :md5_verified,
                :download_date, :download_location,
                :transferred_for_output, :downloaded_by_application);
    """

NEW_HASH_SQL =\
    """INSERT INTO md5_checksums (
            referenced_record, unique_file_id, md5_value,
            download_date, filename, checksum_file_deleted
            ) VALUES (
                :referenced_record, :unique_file_id, :md5_value,
                :download_date, :filename, :checksum_file_deleted
                );
    """

NEW_NOTE_SQL =\
    """INSERT INTO archive_notes (
            filename, referenced_record, unique_file_id, download_date
            ) VALUES (
                :filename, :referenced_record, :unique_file_id,
                :download_date);
    """


"""
Examples of an mlsd listing

modify=20131125174213;perm=adfr;size=24847843;type=file;unique=4600001UE9FE;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0745.xml.gz
modify=20131125174556;perm=adfr;size=63;type=file;unique=4600001UEA02;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0002.xml.gz.md5
"""


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
    return FTPFileParams(
        metadata['modify'], metadata['size'], metadata['unique'], filename)


def get_file_listing(connection, server_dir, skip_patterns=None):
    """
    Returns tuples of file information for each file in server_dir.

    Directories are automatically removed from this listing (see
    `parse_mlsd`).

    `skip_patterns` can be a collection of regex patterns that match
    undesired files. It defaults to `(r'stats\.html$', r'\.dat$')`.
    """
    # This should definitely be done line-wise but ftplib complains when
    # the NLM server changes mode from BINARY to ASCII and back again.
    # Using StringIO is a workaround.
    #
    # Eventually, should try using ftplib sendcmd to change mode before
    # requesting a test file and reset the mode before requesting a
    # binary file.
    file_listing = StringIO.StringIO()
    connection.retrbinary('MLSD %s' % server_dir, file_listing.write)
    file_listing.seek(0)

    if skip_patterns is None:
        skip_patterns = (r'stats\.html$', r'\.dat$')
    skip_patterns = tuple(re.compile(p) for p in skip_patterns)

    for line in file_listing.readlines():
        listing = parse_mlsd(line)
        if listing is None:
            continue
        for p in skip_patterns:
            if p.match(listing.filename) is not None:
                continue
        yield listing


def initialize_download_database_connection(db_file):
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


def get_downloaded_file_unique_ids():
    """
    Returns a set of identifers for previously downloaded files.
    """
    db_con = DOWNLOADED_FILES_DATABASE_CONNECTION
    downloaded_files = {row['unique_file_id'] for row in db_con.execute(
        "SELECT unique_file_id FROM nlm_archives")}
    downloaded_files.union({row['unique_file_id'] for row in db_con.execute(
        "SELECT unique_file_id FROM md5_checksums")})
    downloaded_files.union({row['unique_file_id'] for row in db_con.execute(
        "SELECT unique_file_id FROM archive_notes")})
    return downloaded_files


def record_downloads(downloads_list):
    """
    Update downloaded files database with files from downloads_list.

    `downloads_list` is a collection of dictionaries with keys from
    `FTPFileParams`, augmented with the key `download_date`.
    """
    download_types = {'archive': [], 'hash': [], 'note': []}
    for download in downloads_list:
        referenced_record = MEDLINE_ARCHIVE_PATTERN.match(download['filename'])
        if referenced_record is not None:
            download['referenced_record'] = referenced_record.group(0)
        else:
            # This can only happen for notes - see the DB schema
            download['referenced_record'] = None
        filename = download['filename']
        if filename.endswith('.xml.gz'):
            download.update(md5_verified=0, transferred_for_output=0,
                            downloaded_by_application=0)
            download_types['archive'].append(download)
        elif filename.endswith('.xml.gz.md5'):
            # XXX Should delete these hash files eventually
            with open(downloads_list['output_path']) as hash_file:
                md5_value = hash_file.read()
            download.update(md5_value=md5_value, checksum_file_deleted=0)
            download_types['hash'].append(download)
        else:
            download_types['note'].append(download)
    db_con = DOWNLOADED_FILES_DATABASE_CONNECTION
    db_con.executemany(NEW_ARCHIVE_SQL, download_types['archive'])
    # TODO Enforce foreign key constraints on hashes and notes
    db_con.executemany(NEW_HASH_SQL, download_types['hash'])
    db_con.executemany(NEW_NOTE_SQL, download_types['note'])
    db_con.commit()


def retrieve_nlm_files(connection, server_dir, output_dir, limit=0):
    """
    Download new files from path `server_dir` to `output_dir`.
    Only retrieve `limit` files if limit is greater than 0.

    `connection` is an ftplib.FTP object.
    """
    connection.cwd(server_dir)
    downloaded_files = get_downloaded_file_unique_ids()
    retrieved_files = []
    output_dir = path.abspath(output_dir)
    try:
        for i, file_info in enumerate(
                get_file_listing(connection, server_dir)):
            # XXX Just for debugging
            if limit > 0 and i > limit:
                break
            if file_info.unique_file_id in downloaded_files:
                continue
            output_path = path.join(output_dir, file_info.filename)
            with open(output_path, 'wb+') as new_file:
                connection.retrbinary(
                    'RETR %s' % file_info.filename, new_file.write)
                new_file.seek(0)
                observed_md5 = hashlib.md5()
                observed_md5.update(new_file.read())
            file_info_dict = file_info._asdict()
            file_info_dict.update(
                download_date=time.strftime('%Y%m%d%H%M%S'),
                observed_md5=observed_md5.digest(),
                output_path=output_path)
            retrieved_files.append(file_info_dict)
    finally:
        # Record successful downloads even after a download failure
        record_downloads(retrieved_files)
    return 'Success'


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="""Script to download new files from the NLM public
                       FTP server.
                    """)

    parser.add_argument(
        'server_data_dir',
        help='Directory containing desired files on the NLM FTP server')

    parser.add_argument(
        '-n', '--netrc', default='~/.netrc',
        help="""netrc file containing login parameters for the NLM
                server. See `man 5 netrc` for details on generating this
                file.
             """)

    parser.add_argument(
        '-d', '--download_database', default='~/.ftp_download_db',
        help='Path to SQLite database detailing past downloads')
    parser.add_argument(
        '-o', '--output_dir', default='~/medline_data',
        help='Directory where downloads will be saved')
    parser.add_argument(
        '-l', '--limit', type=int, default=0,
        help='Only download LIMIT files.')

    parser.add_argument(
        '-x', '--export_dir', default='~/medline_data_exports',
        help="""Directory where data to be retrieved by the
                `hypothesis_graph application server are staged.
             """)

    args = parser.parse_args()

    NLM_NETRC = netrc.netrc(file=path.expanduser(args.netrc))
    assert len(NLM_NETRC.hosts.keys()
               ) == 1, "The netrc file should contain only one record"
    for server, params in NLM_NETRC.hosts.items():
        FTP_PARAMS = FTPConnectionParams(*([server] + list(params)))

    FTP_CONNECTION = ftplib.FTP(
        host=FTP_PARAMS.host, user=FTP_PARAMS.user, passwd=FTP_PARAMS.password)
    DOWNLOADED_FILES_DATABASE_CONNECTION =\
        initialize_download_database_connection('test.db')

    # TODO Wrap this in an exception handler that sends an email
    # on failure
    assert retrieve_nlm_files(connection=FTP_CONNECTION,
                              server_dir=args.server_data_dir,
                              output_dir=args.output_dir,
                              limit=args.limit) == 'Success'

    # TODO Move the downloaded files to the export directory and send an
    # email when this is done
