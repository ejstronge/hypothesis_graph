
"""
db.py - Retrieve and persist Medline records
"""
import sqlalchemy
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
# Not going to use this here but may need it in an importing module
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.types import Date


Base = declarative_base()

DB_LOCATION = 'sqlite:////home/roi/work/script/hscrb_arlotta/data_retrieval/ISH/data/'\
    'img_availability.db'
Engine = sqlalchemy.create_engine(DB_LOCATION, echo=False)
Session = sessionmaker(bind=Engine)


def create_all_tables():
    Base.metadata.create_all(Engine)


class Article(Base):

    __tablename__ = 'article'
    __slots__ = []

    id = Column(Integer, primary_key=True)
    pmid_version = Column(Integer, nullable=False)
    pmc_id = Column(Integer)
    title = Column(String, nullable=False)
    abstract = Column(String)
    # XXX many-to-many
    authors = Column(String, ForeignKey('author.id'))
    journal = Column(String, nullable=False)
    pub_date = Column(Date, nullable=False)
    types = Column(Integer, ForeignKey('article_type.id'))
    key_terms = Column(String)

    article_types = relationship('article_type')


class ArticleType(Base):

    __tablename__ = 'article_type'
    __slots__ = []

    id = Column(Integer, primary_key=True)
    description = Column(String, nullable=False)
    # Eventually add weight values to each article type
    # to reflect their importance in establishing a connection
    # between two researchers


class Author(Base):

    __tablename__ = 'author'
    __slots__ = []

    id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    affiliation = Column(String)  # TODO Make this one-to-many


class Journal(Base):

    __tablename__ = 'journal'
    __slots__ = []

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    abbreviation = Column(String)
    issn = Column(String)
    issn_medium = Column(String)
    nlm_unique_id = Column(String)
