
"""
parse_medline_data.py - Parse Medline XML files
"""
from collections import namedtuple
import re
import string

from lxml import etree


MIN_AUTHOR_COUNT = 2
AuthorName = namedtuple('AuthorName', 'initials surname full_name')

UNICODE_TRANSLATION_TABLE = dict(
    (ord(char), None) for char in string.punctuation)
UNICODE_TRANSLATION_TABLE.update(dict(
    (ord(s), unicode(s.lower())) for s in string.uppercase))

ASCII_TRANSLATION_TABLE = string.maketrans(
    string.punctuation + string.uppercase,
    ' ' * len(string.punctuation) + string.lower(string.uppercase))

class AuthorCountException(Exception):
    pass


class PublicationTypeException(Exception):
    pass


# XXX Need to update this to look for each author's affiliation
# with the 2014 DTD
def _process_authors(el, affiliations_el=None):
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


def _process_keywords(el, set_type):
    # XXX Need to rearrange terms containing commas
    descriptor_path = 'MeshHeading/DescriptorName[@MajorTopicYN="%s"]'
    qualifier_path = 'MeshHeading/QualifierName[@MajorTopicYN="%s"]'
    keyword_path = 'Keyword[@MajorTopicYN="%s"]'
    if set_type == 'MeshHeadingList':
        xpaths = (descriptor_path, qualifier_path)
    elif set_type == 'KeywordList':
        xpaths = (keyword_path, )

    # Avoiding a list comprehension because we may get differing types
    # of strings back from our xpath query
    major_terms, minor_terms = [], []
    for terms, importance in ((major_terms, 'Y'), (minor_terms, 'N')):
        for p in xpaths:
            for i in el.xpath(p % importance):
                i = i.text
                if type(i) == type(u''):
                    terms.append(i.translate(UNICODE_TRANSLATION_TABLE))
                else:
                    terms.append(string.translate(i, ASCII_TRANSLATION_TABLE))
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


def _process_article(el):
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


def _date_string_from_element(el):
    """
    Given an XML element with children Year, Month and Day, returns
    a YYYY-MM-DD string.
    """
    return "%(Year)s-%(Month)s-%(Day)s" % dict(zip(
        [c.tag for c in el.getchildren()], [c.text for c in el.getchildren()]))


_SHORT_MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug',
                      'Sep', 'Oct', 'Nov', 'Dec']
_LONG_MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
    'September', 'October', 'November', 'December']

DATE_TRANSLATIONS = {}
for names in (_SHORT_MONTH_NAMES, _LONG_MONTH_NAMES):
    DATE_TRANSLATIONS.update(
        {month: sequence for month, sequence in zip(names, range(1, 13))})

YEAR_RE_PATTERN = re.compile('\d{4}')


def _process_journal_info(el):
    """
    Process the Journal element within NLM Article elements.
    (http://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#journal)
    """
    assert len(el) == 1  # We only expect one journal record per article
    el = el[0]

    title = el.xpath('string(Title)')
    abbreviation = el.xpath('string(ISOAbbreviation)')
    # Some journals will have an NlmUniqueID and no ISSN. In every case where
    # an NlmUniqueID is present it will be used instead of the ISSN
    issn = el.xpath('string(ISSN)') or None
    # Date parsing should be interesting - there's no guarantee on what
    # fields will be present so I may need to do some ugly stuff
    pub_date_el = el.xpath('JournalIssue/PubDate')[0]
    date_elements = set([c.tag for c in pub_date_el.getchildren()])

    final_pub_date = {'Year': 1900, 'Month': 1, 'Day': 1}
    if 'MedlineDate' in date_elements:
        date_text = pub_date_el.xpath('string(MedlineDate)')
        year = re.search(YEAR_RE_PATTERN, date_text)
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
            'id': issn,
            'id_type': 'issn',
            'pub_date': final_pub_date,
            }


# XXX remaining attributes: comments/corrections;
#
# Could also use citation list from PMC articles... This really only becomes
# interesting when an author could cite a set of related articles but
# only chooses to cite a subset. This could be especially interesting for
# a review in PMC.
def parse_medline_xml_file(medline_xml):
    """
    Parse target information from file `medline_xml`
    """
    parser = etree.XMLParser(encoding='utf-8')
    tree = etree.parse(medline_xml, parser=parser)
    for citation in tree.iter(tag='MedlineCitation'):
        all_tags = [c.tag for c in citation.getchildren()]

        # Article identifiers
        pmc = citation.xpath("string(OtherID[@Source='NLM'])") or None
        pmid = int(citation.xpath('string(PMID)'))
        pmid_version = int(citation.xpath('string(PMID/@Version)'))
        if pmc is not None and 'Available on' in pmc:
            # Record isn't yet accessible. See
            # http://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#otherid
            # TODO Need to indicate the temporary inavailability somehow...
            pmc = pmc.split()[0]

        # Article information
        try:
            article_info = _process_article(citation.xpath('Article'))
        except PublicationTypeException:
            continue
        for date in ('DateCreated', 'DateCompleted', 'DateRevised'):
            if date in all_tags:
                record_modification_date = _date_string_from_element(citation.xpath(date)[0])

        # Journal information
        journal_info = _process_journal_info(citation.xpath('Article/Journal'))
        if 'MedlineJournalInfo' in all_tags:
            journal_info['id'] = citation.xpath('string(MedlineJournalInfo/NlmUniqueID)')
            journal_info['id_type'] = 'NlmUniqueID'

        # Author names - note we're only interested in publications with at
        # least two authors
        try:
            author_info = _process_authors(
                citation.xpath('Article/AuthorList'),
                # This function needs to change w/ 2014 DTD
                affiliations_el=citation.xpath('Article/Affiliation'))
        except AuthorCountException:
            continue

        # Keywords
        # Many records have two sets of keywords; I'll only keep once since
        # the two sets often have terms in common, which could unduly
        # influence search results
        for keyword_set in citation.xpath(
                'KeywordList') + citation.xpath('MeshHeadingList'):
            keywords = _process_keywords(keyword_set, set_type=keyword_set.tag)

        # TODO take comments/corrections into consideration
        yield {'article': article_info,
               'pmcid': pmc,
               'pmid': {'id': pmid, 'version': pmid_version},
               'modification_date': record_modification_date,
               'journal': journal_info,
               'author': author_info,
               'keywords': keywords}
