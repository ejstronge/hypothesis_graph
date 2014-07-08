
"""
__init__.py - tools for testing Medline data parsing
"""
import os

from lxml import etree

import parse_medline_data


MEDLINE_FILES = [
    '../data/medline13n0033.xml.gz',
    '../data/medline13n0073.xml',
    '../data/medline13n0143.xml',
    '../data/medline13n0236.xml',
    '../data/medline13n0363.xml',
    '../data/medline13n0438.xml',
    '../data/medline13n0551.xml',
    '../data/medline13n0701.xml',
    '../data/medsamp2013.xml',
    ]

MEDLINE_FILES = [
    os.path.join(os.path.dirname(__file__), p) for p in MEDLINE_FILES]


def is_valid_xml(medline_xml):
    """ is_valid_xml -> bool

    Validates medline_xml using its internally referenced DTD.
    """
    parser = etree.XMLParser(load_dtd=True, no_network=False)
    tree = etree.parse(medline_xml, parser)
    dtd = tree.docinfo.externalDTD
    return dtd.validate(tree)


def test_parsing_run(medline_xml):
    parse_medline_data.parse_medline_xml_file(medline_xml)
