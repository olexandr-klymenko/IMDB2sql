from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base(metadata=MetaData())

NameTitle = Table('name_title',
                  Base.metadata,
                  Column('name_id', Integer, ForeignKey('name.id')),
                  Column('title_id', Integer, ForeignKey('title.id'))
                  )

ProfessionName = Table('profession_name',
                       Base.metadata,
                       Column('profession_id', Integer, ForeignKey('profession.id')),
                       Column('name_id', Integer, ForeignKey('name.id'))
                       )

GenreTitle = Table('genre_title',
                   Base.metadata,
                   Column('genre_id', Integer, ForeignKey('genre.id')),
                   Column('title_id', Integer, ForeignKey('title.id')),
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

    names = relationship("Name", secondary=NameTitle, backref='title')
    genres = relationship("Genre", secondary=GenreTitle, backref='title')


class Name(Base):
    __tablename__ = 'name'

    id = Column(Integer, primary_key=True)
    primary_name = Column(String(120))
    birth_year = Column(Integer)
    death_year = Column(Integer, nullable=True)

    titles = relationship("Title", secondary=NameTitle, backref='name')
    professions = relationship("Profession", secondary=ProfessionName, backref='name')


class Principal(Base):
    __tablename__ = 'principal'

    id = Column(Integer, primary_key=True)
    job = Column(String(20))

    title_id = Column(Integer, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)

    name_id = Column(Integer, ForeignKey('name.id'))
    name = relationship("Name", uselist=False)


class Rating(Base):
    __tablename__ = 'rating'

    id = Column(Integer, primary_key=True)
    average_rating = Column(Float)
    num_votes = Column(Integer)

    title_id = Column(Integer, ForeignKey('title.id'))
    title = relationship("Title", uselist=False)


class Profession(Base):
    __tablename__ = 'profession'

    id = Column(Integer, primary_key=True)
    profession = Column(String(50), nullable=False)

    names = relationship("Name", secondary=ProfessionName, backref='profession')


class Genre(Base):
    __tablename__ = 'genre'

    id = Column(Integer, primary_key=True)
    genre = Column(String(50), nullable=False)

    titles = relationship("Title", secondary=GenreTitle, backref='genre')
