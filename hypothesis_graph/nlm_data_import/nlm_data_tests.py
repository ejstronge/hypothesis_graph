
"""
nlm_data_tests.py
=================

Basic tests for archives downloaded from NLM. All test parameters
are taken from archive descriptions; see
http://www.nlm.nih.gov/bsd/licensee/2014_stats/baseline_doc.html for
details.
"""

import os
from os import path

from lxml import etree


_2014_MEDLINE_FILES_DIRECTORY = path.join(
    path.dirname(__file__), '../data/2014')
MEDLINE_FILES = [
    path.join(_2014_MEDLINE_FILES_DIRECTORY, p) for p in os.listdir(
        _2014_MEDLINE_FILES_DIRECTORY)]


def has_30k_or_fewer_records(medline_xml, parser=None, tree=None):
    """Return whether medline_xml contains 30,000 records or fewer

    Medline XML records contain at most 30k MedlineCitation elements.
    This is a simple check for all new files.
    """


def is_valid_xml(medline_xml, parser=None, tree=None):
    """Return whether medline_xml is valid by checking its dtd

    Validates medline_xml using its internally referenced DTD.
    """
    if parser is None:
        parser = etree.XMLParser(load_dtd=True, no_network=False)
    if tree is None:
        tree = etree.parse(medline_xml, parser)
    dtd = tree.docinfo.externalDTD
    return dtd.validate(tree)
