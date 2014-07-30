# -*- coding: utf-8 -*-
"""
test_nlm_data_import.py
=======================

Test suite for the archive downloading module

(c) 2014, Edward J. Stronge
Available under the GPLv3 - see LICENSE for details.
"""
from collections import namedtuple
import unittest

from ..nlm_data_import import nlm_downloads_db as downloads_db


class TestNLMDatabase(unittest.TestCase):
    """Test that the database for downloaded files can be created
    successfully and works as expected.
    """
    NLMArchiveData = namedtuple(
        'NLMArchiveData',
        """size referenced_record filename unique_file_id modification_date
        observed_md5 md5_verified download_date download_location
        transferred_for_output export_location downloaded""")

    nlm_archive_test_records = [
        NLMArchiveData(*fields) for fields in
        (
            (93, 'nlm1', 'nlm1.xml.tar.gz', 'unique1', '20140731', 'hash1', 0, '20140731', 'downloads/', 0, None, 0),
            (92, 'nlm2', 'nlm2.xml.tar.gz', 'unique2', '20140722', 'hash2', 0, '20240732', 'downloads/', 0, None, 0),
            (93, 'nlm3', 'nlm3.xml.tar.gz', 'unique3', '20140723', 'hash3', 0, '30340733', 'downloads/', 0, None, 0),
            (94, 'nlm4', 'nlm4.xml.tar.gz', 'unique4', '20140714', 'hash4', 0, '40440744', 'downloads/', 0, None, 0),
            (95, 'nlm5', 'nlm5.xml.tar.gz', 'unique5', '20150705', 'hash5', 0, '50550755', 'downloads/', 0, None, 0),
        )]

    NLMHashData = namedtuple(
        'NLMHashData',
        """referenced_record, unique_file_id, md5_value download_date
        filename checksum_file_deleted""")
    nlm_hash_test_records = [
        NLMHashData(*fields) for fields in
        (
            ('nlm1', 'unique-hash1', 'hash1', '20140731', 'nlm1.xml.tar.gz.md5', 0),
            ('nlm2', 'unique-hash2', 'hash2', '20140702', 'nlm2.xml.tar.gz.md5', 0),
            ('nlm3', 'unique-hash3', 'hash3', '30140703', 'nlm3.xml.tar.gz.md5', 0),
            ('nlm4', 'unique-hash4', 'hash4', '40140714', 'nlm4.xml.tar.gz.md5', 0),
            ('nlm5', 'unique-hash5', 'hash5', '20140715', 'nlm5.xml.tar.gz.md5', 0),
        )]

    NLMNoteData = namedtuple(
        'NLMNoteData',
        'filename referenced_record unique_file_id download_date')
    nlm_note_test_records = [
        NLMNoteData(*fields) for fields in
        (
            ('special-note.txt', None, 'repeated-note1', '20140731'),
            ('nlm1-retracted.txt', 'nlm1', 'unique-note1', '20140731'),
            ('nlm4-revised.txt', 'nlm4', 'unique-note4', '20140704'),
        )]

    def setUp(self):
        """Make an in-memory database connection and load the downloads
        schema.
        """
        self.test_db = downloads_db.initialize_database_connection(':memory:')

    def generate_records(self):
        """ Insert test records into the test database.
        """
        self.test_db.executemany(
            downloads_db.NEW_ARCHIVE_SQL, self.nlm_archive_test_records)
        self.test_db.executemany(
            downloads_db.NEW_HASH_SQL, self.nlm_hash_test_records)
        self.test_db.executemany(
            downloads_db.NEW_NOTE_SQL, self.nlm_note_test_records)

    def test_insert_nlm_archive(self):
        """Test insertion of new nlm archive files to the downloads
        database.
        """
        self.test_db.execute(downloads_db.NEW_ARCHIVE_SQL, {
            'size':  1,
            'referenced_record': 'medline14n001',
            'filename': 'f',
            'unique_file_id': 'A',
            'modification_date': '20131128175433',
            'observed_md5': 'abcdefgh',
            'md5_verified': 0,
            'download_date': '20140702134531',
            'download_location': 'downloads',
            'transferred_for_output': 0,
            'export_location': 'exports',
            'downloaded_by_application': 0})
        self.test_db.commit()

    def test_insert_md5_checksum(self):
        """Test insertion of an md5 hash to the downloads database."""
        self.test_db.execute(downloads_db.NEW_HASH_SQL, {
            'referenced_record': 'medline14n001',
            'unique_file_id': 'A',
            'md5_value': 'abcdefgh',
            'download_date': '20140702134531',
            'filename': 'test_file.md5',
            'checksum_file_deleted': 0})
        self.test_db.commit()

    def test_insert_note(self):
        """Test insertion of an auxiliary file to the downloads database."""
        self.test_db.execute(downloads_db.NEW_NOTE_SQL, {
            'filename': 'test_note.txt', 'referenced_record': 'medline14n001',
            'unique_file_id': 'A', 'download_date': '20140702134531', })
        self.test_db.commit()

    def test_get_downloaded_file_unique_ids(self):
        """Test whether we correctly identify all the unique file
        hashes present in the database.
        """
        self.generate_records()

        unique_ids = {record[3] for record in self.nlm_archive_test_records}
        for record_set in (self.nlm_hash_test_records,
                           self.nlm_note_test_records):
            for record in record_set:
                unique_ids.add(record.unique_file_id)

        self.assertSetEqual(
            downloads_db.get_downloaded_file_unique_ids(self.test_db),
            unique_ids)
