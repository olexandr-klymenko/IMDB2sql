from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, Table, MetaData

__all__ = ['Title', 'Name', 'Principals']

Base = declarative_base(metadata=MetaData())

NameTitle = Table('NameTitle',
                  Base.metadata,
                  Column('nameId', Integer, ForeignKey('name.id')),
                  Column('titleId', Integer, ForeignKey('title.id'))
                  )


class Title(Base):
    __tablename__ = 'title'

    id = Column(String, primary_key=True)
    titleType = Column(String)
    primaryTitle = Column(String)
    originalTitle = Column(String)
    isAdult = Column(Boolean)
    startYear = Column(Integer)
    endYear = Column(Integer, nullable=True)
    runtimeMinutes = Column(Integer)
    genres = Column(String)

    names = relationship("Name", secondary=NameTitle, backref='title')


class Name(Base):
    __tablename__ = 'name'

    id = Column(String, primary_key=True)
    primaryName = Column(String)
    birthYear = Column(Integer)
    deathYear = Column(Integer, nullable=True)
    primaryProfession = Column(String)

    titles = relationship("Title", secondary=NameTitle, backref='name')


class Principals(Base):
    __tablename__ = 'principals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ordering = Column(Integer)
    category = Column(String)
    job = Column(String)
    characters = Column(String)

    title_id = Column(String, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)

    name_id = Column(String, ForeignKey('name.id'))
    name = relationship("Name", uselist=False)
