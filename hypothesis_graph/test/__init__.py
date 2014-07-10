
"""
__init__.py - tools for testing Medline data parsing
"""
import os
from os import path

from lxml import etree

import parse_medline_data


_2014_MEDLINE_FILES_DIRECTORY = path.join(
    path.dirname(__file__), '../data/2014')
MEDLINE_FILES = [
    path.join(_2014_MEDLINE_FILES_DIRECTORY, p) for p in os.listdir(_2014_MEDLINE_FILES_DIRECTORY)]


def has_30k_or_fewer_records(medline_xml, parser=None, tree=None):
    """ has_30k_or_fewer_records -> bool

    Medline XML records contain at most 30k MedlineCitation elements.
    This is a simple check for all new files.
    """
    pass


def is_valid_xml(medline_xml, parser=None, tree=None):
    """ is_valid_xml -> bool

    Validates medline_xml using its internally referenced DTD.
    """
    if parser is None:
        parser = etree.XMLParser(load_dtd=True, no_network=False)
    if tree is None:
        tree = etree.parse(medline_xml, parser)
    dtd = tree.docinfo.externalDTD
    return dtd.validate(tree)


def test_parsing_run(medline_xml):
    parse_medline_data.parse_medline_xml_file(medline_xml)
