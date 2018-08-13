from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, Table, MetaData


Base = declarative_base(metadata=MetaData())

NameTitle = Table('NameTitle',
                  Base.metadata,
                  Column('nameId', Integer, ForeignKey('name.id')),
                  Column('titleId', Integer, ForeignKey('title.id'))
                  )


class Name(Base):
    __tablename__ = 'name'

    id = Column(String, primary_key=True)
    primaryName = Column(String)
    birthYear = Column(Integer)
    deathYear = Column(Integer, nullable=True)
    primaryProfession = Column(String)

    titles = relationship("Title", secondary=NameTitle, backref='name')


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
