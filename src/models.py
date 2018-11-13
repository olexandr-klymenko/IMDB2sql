from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, Table, MetaData

__all__ = ['Title', 'Name', 'Principals']

Base = declarative_base(metadata=MetaData())

NameTitle = Table('NameTitle',
                  Base.metadata,
                  Column('nameId', Integer, ForeignKey('name.id')),
                  Column('titleId', Integer, ForeignKey('title.id'))
                  )


class Title(Base):
    __tablename__ = 'title'

    id = Column(Integer, primary_key=True)
    titleType = Column(String(20))
    primaryTitle = Column(String(450))
    originalTitle = Column(String(450))
    isAdult = Column(Boolean)
    startYear = Column(Integer)
    endYear = Column(Integer, nullable=True)
    runtimeMinutes = Column(Integer)
    genres = Column(String(40))

    names = relationship("Name", secondary=NameTitle, backref='title')


class Name(Base):
    __tablename__ = 'name'

    id = Column(Integer, primary_key=True)
    primaryName = Column(String(120))
    birthYear = Column(Integer)
    deathYear = Column(Integer, nullable=True)
    primaryProfession = Column(String(70))

    titles = relationship("Title", secondary=NameTitle, backref='name')


class Principals(Base):
    __tablename__ = 'principals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ordering = Column(Integer)
    category = Column(String(30))
    job = Column(String(30))
    characters = Column(String(30))

    title_id = Column(Integer, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)

    name_id = Column(Integer, ForeignKey('name.id'))
    name = relationship("Name", uselist=False)


class Ratings(Base):
    __tablename__ = 'ratings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    averageRating = Column(Float)
    numVotes = Column(Integer)

    title_id = Column(Integer, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)
