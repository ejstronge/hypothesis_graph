
"""
utils.py - tools for testing Medline data parsing
"""

from lxml import etree


MEDLINE_FILES = [
    'sample_medline_files/medline13n0033.xml.gz',
    'sample_medline_files/medline13n0073.xml',
    'sample_medline_files/medline13n0143.xml',
    'sample_medline_files/medline13n0236.xml',
    'sample_medline_files/medline13n0363.xml.gz',
    'sample_medline_files/medline13n0438.xml',
    'sample_medline_files/medline13n0551.xml',
    'sample_medline_files/medline13n0701.xml',
    'sample_medline_files/medsamp2013.xml',
    ]


def is_valid_xml(medline_xml):
    """ is_valid_xml -> bool

    Validates medline_xml using its internally referenced DTD.
    """
    parser = etree.XMLParser(load_dtd=True)
    tree = etree.parse(medline_xml, parser)
    dtd = tree.docinfo.internalDTD
    return dtd.validate(tree.getroot())
