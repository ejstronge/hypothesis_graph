
"""
utils.py - tools for testing Medline data parsing
"""
from collections import namedtuple
import random
import re
import string

from lxml import etree


class AuthorCountException(Exception):
    pass


class PublicationTypeException(Exception):
    pass


MEDLINE_FILES = [
    'sample_medline_data/medline13n0033.xml.gz',
    'sample_medline_data/medline13n0073.xml',
    'sample_medline_data/medline13n0143.xml',
    'sample_medline_data/medline13n0236.xml',
    'sample_medline_data/medline13n0363.xml.gz',
    'sample_medline_data/medline13n0438.xml',
    'sample_medline_data/medline13n0551.xml',
    'sample_medline_data/medline13n0701.xml',
    'sample_medline_data/medsamp2013.xml',
    ]


def is_valid_xml(medline_xml):
    """ is_valid_xml -> bool

    Validates medline_xml using its internally referenced DTD.
    """
    parser = etree.XMLParser(load_dtd=True, no_network=False)
    tree = etree.parse(medline_xml, parser)
    dtd = tree.docinfo.externalDTD
    return dtd.validate(tree)


MIN_AUTHOR_COUNT = 2
AuthorName = namedtuple('AuthorName', 'initials surname full_name')


# XXX Need to update this to look for each author's affiliation
# with the 2014 DTD
def process_authors(el, affiliations_el=None):
    """

    """
    if not el:
        raise AuthorCountException
    el = el[0]
    author_el = el.xpath('Author')
    if len(author_el) < MIN_AUTHOR_COUNT:
        raise AuthorCountException
    authors = []
    for a in author_el:
        surname = a.xpath('string(LastName)')
        forename = a.xpath('string(ForeName)')
        # initials doesn't include the surname
        initials = a.xpath('string(Initials)')
        full_name = "%s %s" % (forename, surname)
        authors.append(AuthorName(initials, surname, full_name))
    affiliations = []
    if affiliations_el is not None and affiliations_el:
        affiliations = [a.text for a in affiliations_el]
    return {'complete': True if el.get('CompleteYN') == 'Y' else False,
            'authors': authors,
            'affiliations': affiliations,
            }

KEYWORD_TRANSLATION_TABLE = string.maketrans(
    string.punctuation + string.uppercase,
    ' ' * len(string.punctuation) + string.lower(string.uppercase))


def process_keywords(el, set_type):
    # XXX Need to rearrange terms containing commas
    if set_type == 'MeshHeadingList':
        major_terms = [string.translate(i.text, KEYWORD_TRANSLATION_TABLE
                                        ) for i in el.xpath(
            'MeshHeading/DescriptorName[@MajorTopicYN="Y"]') + el.xpath(
            'MeshHeading/QualifierName[@MajorTopicYN="Y"]')]
        minor_terms = [string.translate(i.text, KEYWORD_TRANSLATION_TABLE
                                        ) for i in el.xpath(
            'MeshHeading/DescriptorName[@MajorTopicYN="N"]') + el.xpath(
            'MeshHeading/QualifierName[@MajorTopicYN="N"]')]
    elif set_type == 'KeywordList':
        major_terms = [string.translate(i.text, KEYWORD_TRANSLATION_TABLE
            ) for i in el.xpath('Keyword[@MajorTopicYN="Y"]')]
        minor_terms = [string.translate(i.text, KEYWORD_TRANSLATION_TABLE
            ) for i in el.xpath('Keyword[@MajorTopicYN="N"]')]
    return {'major_terms': major_terms, 'minor_terms': minor_terms}


# PUBLICATION TYPES
#
# To save record processing time, we'll be selective about what publication
# types are recorded. I'll come back later to see if the types of publication
# we ignore end up being interesting.
#
# See http://www.nlm.nih.gov/mesh/pubtypes.html for a complete list of
# publication types. THESE NEED TO BE REVISITED YEARLY!
#
# ** PUBLICATON TYPES THAT PROBABLY NEED SPECIAL ATTENTION **
#
#   Addresses - should have stronger weight on coauthorship
#   Biography - probably need a special weight, maybe stronger?
#   Comment - need to look at the referred-to publication
#   Congresses
#   Consensus Development Conference
#   Consensus Development Conference, NIH
#   Duplicate Publication
#   Editorial
#   Historical Article
#   Lectures
#   Letter
#   Personal Narratives
#   Practice Guideline
#
# XXX For Abstracts, need to see how the author lists are determined. These
# could be especially interesting or simply noisy
#
# Patents aren't indexed by Pubmed but could be interesting to include out-of
# -band
DESIRED_PUBLICATION_TYPES = {
    'Abstracts', 'Academic Dissertations', 'Addresses', 'Advertisements',
    'Biography', 'Case Reports', 'Classical Article', 'Clinical Conference',
    'Clinical Trial', 'Clinical Trial, Phase I', 'Clinical Trial, Phase II',
    'Clinical Trial, Phase III', 'Clinical Trial, Phase IV', 'Collected Works',
    'Comment', 'Comparative Study', 'Congresses', 'Consensus Development Conference',
    'Consensus Development Conference, NIH', 'Controlled Clinical Trial',
    'Corrected and Republished Article', 'Dictionary', 'Duplicate Publication',
    'Editorial', 'English Abstract', 'Evaluation Studies', 'Festschrift',
    'Guideline', 'Historical Article', 'In Vitro', 'Introductory Journal Article',
    'Journal Article', 'Lectures', 'Legal Cases', 'Legislation', 'Letter',
    'Meta-Analysis', 'Multicenter Study', "Nurses' Instruction", 'Observational Study',
    'Overall',  'Personal Narratives', 'Practice Guideline', 'Pragmatic Clinical Trial',
    'Published Erratum', 'Randomized Controlled Trial', 'Retracted Publication',
    'Retraction of Publication', 'Review', 'Technical Report', 'Twin Study',
    'Validation Studies',
    }
EXCLUDED_PUBLICATION_TYPES = {
    'Autobiography',
    # There seems to be a French language bias in bibliography publication...
    'Bibliography',
    }


def process_article(el):
    """
    Processes the Article element within NLM records
    (http://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#article)
    """
    assert len(el) == 1  # We only expect one article record
    el = el[0]

    all_tags = [c.tag for c in el.getchildren()]
    title = el.xpath('ArticleTitle')[0].text
    publication_types = [
        pt.text for pt in el.xpath('PublicationTypeList/PublicationType')]
    if all([pt not in DESIRED_PUBLICATION_TYPES for pt in publication_types]
           ) or \
            any([pt in EXCLUDED_PUBLICATION_TYPES for pt in publication_types]
                ):
        raise PublicationTypeException
    # Only keep the publication types we're interested in
    publication_types = set(
        publication_types).intersection(DESIRED_PUBLICATION_TYPES)
    abstract = el.xpath(
        'Abstract/AbstractText')[0].text if 'Abstract' in all_tags else None
    return {'title': title,
            'abstract': abstract,
            'publication_types': publication_types}


def date_string_from_element(el):
    """
    Given an XML element with children Year, Month and Day, returns
    a YYYY-MM-DD string.
    """
    return "%(Year)s-%(Month)s-%(Day)s" % dict(zip(
        [c.tag for c in el.getchildren()], [c.text for c in el.getchildren()]))


DATE_TRANSLATIONS = {
    'Jan': 1,
    'January': 1,
    'Feb': 2,
    'February': 2,
    'Mar': 3,
    'March': 3,
    'Apr': 4,
    'April': 4,
    'May': 5,
    'Jun': 6,
    'June': 6,
    'Jul': 7,
    'July': 7,
    'Aug': 8,
    'August': 8,
    'Sep': 9,
    'September': 9,
    'Oct': 10,
    'October': 10,
    'Nov': 11,
    'November': 11,
    'Dec': 12,
    'December': 12,
    }

YEAR_PATTERN = re.compile('\d{4}')


def process_journal_info(el):
    """
    Process the Journal element within NLM Article elements.
    (http://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#journal)
    """
    assert len(el) == 1  # We only expect one journal record per article
    el = el[0]

    all_tags = [c.tag for c in el.getchildren()]
    title = el.xpath('string(Title)')
    abbreviation = el.xpath('string(ISOAbbreviation)')
    if 'ISSN' in all_tags:
        issn_el = el.xpath('ISSN')[0]
        # XXX Some journals will have an NlmUniqueID; should use this when
        # available.
        issn = issn_el.text
        issn_type = issn_el.get('IssnType')
    else:
        issn = None
        issn_type = 'Undetermined'
    # Date parsing should be interesting - there's no guarantee on what
    # fields will be present so I may need to do some ugly stuff
    pub_date_el = el.xpath('JournalIssue/PubDate')[0]
    date_elements = set([c.tag for c in pub_date_el.getchildren()])

    final_pub_date = {'Year': 1900, 'Month': 1, 'Day': 1}
    if 'MedlineDate' in date_elements:
        date_text = pub_date_el.xpath('string(MedlineDate)')
        year = re.search(YEAR_PATTERN, date_text)
        if year is None:
            # XXX Give up and debug this later. This condition didn't
            # appear in the test data but it could come up when
            # parsing real data
            pass
        else:
            year = year.group(0)
            # Maybe we can guess a month, too
            month = 1
            for k in DATE_TRANSLATIONS.keys():
                # I can't tell if short names are preferred
                # to long names but this shouldn't matter in the end
                if k in date_text:
                    month = DATE_TRANSLATIONS[k]
                    break
            # Let's leave the date alone
            final_pub_date.update(Year=year, Month=month)
    else:
        pub_date = {}
        # Need to build up a date with as much information as we've got
        for k in date_elements.intersection(('Year', 'Month', 'Day')):
            v = pub_date_el.xpath("string(%s)" % k)
            if v in DATE_TRANSLATIONS:
                v = DATE_TRANSLATIONS[v]
            pub_date[k] = int(v)
        final_pub_date.update(pub_date)

    return {'title': title,
            'abbreviation': abbreviation,
            'issn': issn,
            'issn_type': issn_type,
            'pub_date': final_pub_date,
            }


# XXX remaining attributes: comments/corrections;
# to a single text field.
#
# A second approach could be keeping track of which articles have which
# keywords; a keyword match to a search query would then only require
# pulling a list of already-identified articles. This is better than
# collapsing into a single text field as it (may) get around the problem of
# two words being adjacent as an artifact of how I collapsed keywords.
#
# Ultimately, I need to test these approaches.
#
# PubMed seems to prioritize the abstract's text over keyword matches, though
# I could use a keyword partial match to limit my FTS. Let's try this and
# see what happens.
#
# An immediate problem is that a germane article with incomplete tagging might
# be ignored; this could be problematic with earlier publications
#
# Could also use citation list from PMC articles... This really only becomes
# interesting when an author could cite a set of related articles but
# only chooses to cite a subset. This could be especially interesting for
# a review in PMC.
def test_parse(medline_xml):
    """
    Populate database
    """
    tree = etree.parse(medline_xml)
    for citation in tree.iter(tag='MedlineCitation'):
        all_tags = [c.tag for c in citation.getchildren()]
        pmc = citation.xpath("string(OtherID[@Source='NLM'])") or None
        if pmc is not None and 'Available on' in pmc:
            # Record isn't yet accessible. See
            # http://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#otherid
            pmc = pmc.split()[0]
            # XXX Need to indicate the temporary inavailability somehow...

        try:
            article_info = process_article(citation.xpath('Article'))
        except PublicationTypeException:
            continue
        pmid = int(citation.xpath('string(PMID)'))
        pmid_version = int(citation.xpath('string(PMID/@Version)'))
        for date in ('DateCreated', 'DateCompleted', 'DateRevised'):
            if date in all_tags:
                record_modification_date = date_string_from_element(citation.xpath(date)[0])
        journal_info = process_journal_info(citation.xpath('Article/Journal'))
        try:
            author_info = process_authors(
                citation.xpath('Article/AuthorList'),
                # XXX Needs to change w/ 2014 DTD
                affiliations_el=citation.xpath('Article/Affiliation'))
        except AuthorCountException:
            continue
        for keyword_set in citation.xpath(
                'KeywordList') + citation.xpath('MeshHeadingList'):
            # Will only keep one of the two sets of keywords if both exist
            keywords = process_keywords(keyword_set, set_type=keyword_set.tag)
        # XXX TODO need to make sure I'm handling unicode names appropriately
        # TODO take comments/corrections into consideration
        (article_info, pmid, pmid_version, record_modification_date, journal_info, author_info, keywords)
