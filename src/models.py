from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, Table, MetaData

__all__ = ['Title', 'Name', 'Principals']

Base = declarative_base(metadata=MetaData())

NameTitle = Table('name_title',
                  Base.metadata,
                  Column('name_id', Integer, ForeignKey('name.id')),
                  Column('title_id', Integer, ForeignKey('title.id'))
                  )


class Title(Base):
    __tablename__ = 'title'

    id = Column(Integer, primary_key=True)
    title_type = Column(String(20))
    primary_title = Column(String(450))
    original_title = Column(String(450))
    is_adult = Column(Boolean)
    start_year = Column(Integer)
    end_year = Column(Integer, nullable=True)
    runtime_minutes = Column(Integer)
    genres = Column(String(40))

    names = relationship("Name", secondary=NameTitle, backref='title')


class Name(Base):
    __tablename__ = 'name'

    id = Column(Integer, primary_key=True)
    primary_name = Column(String(120))
    birth_year = Column(Integer)
    death_year = Column(Integer, nullable=True)
    primary_profession = Column(String(70))

    titles = relationship("Title", secondary=NameTitle, backref='name')


class Principals(Base):
    __tablename__ = 'principals'

    id = Column(Integer, primary_key=True)
    ordering = Column(Integer)
    category = Column(String(20))
    job = Column(String(300))
    characters = Column(String(500))

    title_id = Column(Integer, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)

    name_id = Column(Integer, ForeignKey('name.id'))
    name = relationship("Name", uselist=False)


class Ratings(Base):
    __tablename__ = 'ratings'

    id = Column(Integer, primary_key=True)
    average_rating = Column(Float)
    num_votes = Column(Integer)

    title_id = Column(Integer, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)
