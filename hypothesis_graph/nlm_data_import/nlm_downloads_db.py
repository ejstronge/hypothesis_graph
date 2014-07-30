
"""
ftp_update_db.py
================

Database functions related to maintaining the list of processed NLM archives.

(c) 2014, Edward J. Stronge.
Available under the GPLv3 - see LICENSE for details.
"""
from os import path
import re
import sqlite3


# example - medline14n0746.xml.gz.md5
MEDLINE_ARCHIVE_PATTERN = re.compile(r'medline\d{2}n\d{4}')


def initialize_database_connection(db_file):
    """Return a connection to `db_file`.

    If the file does not exist it is created and intialized with
    `DOWNLOADED_FILES_SCHEMA`.
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


def get_downloaded_file_unique_ids(db_con):
    """Returns a set of identifers for previously downloaded files."""
    select_statements = (
        "SELECT unique_file_id FROM nlm_archives",
        "SELECT unique_file_id FROM md5_checksums",
        "SELECT unique_file_id FROM archive_notes")
    downloaded_files = set()
    for stmt in select_statements:
        for row in db_con.execute(stmt):
            downloaded_files.add(row['unique_file_id'])
    return downloaded_files


def record_downloads(downloads_list, db_con):
    """Update downloaded files database with files from downloads_list.

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
    db_con.executemany(NEW_ARCHIVE_SQL, download_types['archive'])
    # TODO Enforce foreign key constraints on hashes and notes. See
    # the FK_support branch
    db_con.executemany(NEW_HASH_SQL, download_types['hash'])
    db_con.executemany(NEW_NOTE_SQL, download_types['note'])


def record_files_to_export(exported_record_names, db_con):
    """Mark files in exports_list as having been moved to the export directory.
    """
    db_con.executemany(SET_READY_FOR_EXPORT, exported_record_names)


def check_exported_file_directory(export_dir_files, db_con):
    """Determine if the exported files have been removed by the application
    server.
    """
    # Look for all files that were moved for export; diff vs files
    # that are currently in export dir; set downloaded by
    # downloaded_by_application value
    #files_ready_for_export = db_con.execute(GET_EXPORTED_RECORDS)


##############
# SQL queries
##############
DOWNLOADED_FILES_SCHEMA = """
    /* nlm_archives notes

     transferred_for_output:
         '1' if the the archive has been copied to the export
         directory. The export directory is specified as
         a parameter to the download_nlm_data script.

         '0' by default.

     downloaded_by_application
         '1' if the archive is no longer in the export directory after
         having been transferred.

    */
    CREATE TABLE nlm_archives (
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
        transferred_for_output INTEGER NOT NULL,
        export_location TEXT,
        downloaded_by_application INTEGER NOT NULL
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
        referenced_record TEXT,  -- Refers nlm_archives
        filename TEXT NOT NULL,
        unique_file_id TEXT NOT NULL UNIQUE,
        download_date TEXT NOT NULL
    );
    """

NEW_ARCHIVE_SQL = """
    INSERT INTO nlm_archives (size, record_name, filename, unique_file_id,
        modification_date, observed_md5, md5_verified, download_date,
        download_location, transferred_for_output, export_location,
        downloaded_by_application)

        VALUES (:size, :referenced_record, :filename, :unique_file_id,
            :modification_date, :observed_md5, :md5_verified,
            :download_date, :download_location, :transferred_for_output,
            :export_location, :downloaded_by_application);
    """

NEW_HASH_SQL = """
    INSERT INTO md5_checksums (referenced_record, unique_file_id,
        md5_value, download_date, filename, checksum_file_deleted)

        VALUES (:referenced_record, :unique_file_id, :md5_value,
            :download_date, :filename, :checksum_file_deleted);
    """

NEW_NOTE_SQL = """
    INSERT INTO archive_notes (filename, referenced_record,
        unique_file_id, download_date)

        VALUES (:filename, :referenced_record, :unique_file_id,
            :download_date);
    """

SET_READY_FOR_EXPORT = """
    UPDATE nlm_archives
    SET transferred_for_output=1
    WHERE record_name=:referenced_record;
    """

GET_EXPORTED_RECORDS = """
    SELECT filename
    FROM nlm_archives
    WHERE transferred_for_output=1;
    """

SET_DOWNLOADED_BY_APPLICATION = """
    UPDATE nlm_archives
    SET downloaded_by_application=1
    WHERE record_name=:referenced_record;
    """
