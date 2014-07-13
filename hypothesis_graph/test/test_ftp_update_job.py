# -*- coding: utf-8 -*-
"""
   test_ftp_update_job.py
   =======================

   Test suite for the archive downloading module

   (c) 2014, Edward J. Stronge
   Available under the GPLv3 - see LICENSE for details.
"""
import sqlite3
import unittest

import utils.ftp_update_job as ftp_update


class TestDatabaseScheme(unittest.TestCase):
    """
    Test that the database for downloaded files can be created
    successfully and works as expected.
    """
    def setUp(self):
        """
        Make an in-memory database connection and load the downloads
        schema.
        """
        self.test_db = sqlite3.connect(':memory:')
        self.test_db.executescript(ftp_update.DOWNLOADED_FILES_SCHEMA)
        self.test_db.commit()

    def test_nlm_archive_update(self):
        """
        Test addition of new nlm archive files to the downloads database.
        """
        self.test_db.execute(ftp_update.NEW_ARCHIVE_SQL, {
            'size':  1, 'referenced_record': 'medline14n001', 'filename': 'f',
            'unique_file_id': 'A', 'modification_date': '20131128175433',
            'observed_md5': 'abcdefgh', 'md5_verified': 0,
            'download_date': '20140702134531', 'download_location': 'nlmdata',
            'transferred_for_output': 0, 'downloaded_by_application': 0})
        self.test_db.commit()

    def test_md5_checksum_update(self):
        """
        Test addition of an md5 hash to the downloads database.
        """
        self.test_db.execute(ftp_update.NEW_HASH_SQL, {
            'referenced_record': 'medline14n001', 'unique_file_id': 'A',
            'md5_value': 'abcdefgh', 'download_date': '20140702134531',
            'filename': 'test_file.md5', 'checksum_file_deleted': 0})
        self.test_db.commit()

    def test_note_update(self):
        """
        Test addition of an auxiliary file to the downloads database.
        """
        self.test_db.execute(ftp_update.NEW_NOTE_SQL, {
            'filename': 'test_note.txt', 'referenced_record': 'medline14n001',
            'unique_file_id': 'A', 'download_date': '20140702134531', })
        self.test_db.commit()


class TestMlsdParsing(unittest.TestCase):
    """
    Test parser for the FTP server directory listing
    """

    TEST_LINES = (
        "modify=20131125174213;perm=adfr;size=24847843;type=file;unique=4600001UE9FE;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0745.xml.gz",
        "modify=20131125174556;perm=adfr;size=63;type=file;unique=4600001UEA02;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505; medline14n0002.xml.gz.md5")

    def test(self):
        pass


class TestFileRetrieval(unittest.TestCase):
    pass
