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


class TitleModel(Base):
    __tablename__ = 'title'

    id = Column(Integer, primary_key=True)
    title_type = Column(String(20))
    primary_title = Column(String(450), index=True)
    is_adult = Column(Boolean)
    start_year = Column(Integer)
    end_year = Column(Integer, nullable=True)
    runtime_minutes = Column(Integer)

    names = relationship("NameModel", secondary=NameTitle, backref='title', cascade='delete,all')
    principals = relationship("PrincipalModel", backref='title', cascade='delete,all')
    rating = relationship("RatingModel", backref='title', uselist=False, cascade='delete,all')
    genres = relationship("GenreModel", secondary=GenreTitle, backref='title', cascade='delete,all')


class NameModel(Base):
    __tablename__ = 'name'

    id = Column(Integer, primary_key=True)
    primary_name = Column(String(120), index=True)
    birth_year = Column(Integer)
    death_year = Column(Integer, nullable=True)

    titles = relationship("TitleModel", secondary=NameTitle, backref='name', cascade='delete,all')
    professions = relationship("ProfessionModel", secondary=ProfessionName, backref='name', cascade='delete,all')


class PrincipalModel(Base):
    __tablename__ = 'principal'

    id = Column(Integer, primary_key=True)
    job = Column(String(20))

    title_id = Column(Integer, ForeignKey('title.id'))

    name_id = Column(Integer, ForeignKey('name.id'))
    name = relationship("NameModel", uselist=False, cascade='delete,all')


class RatingModel(Base):
    __tablename__ = 'rating'

    id = Column(Integer, primary_key=True)
    average_rating = Column(Float)
    num_votes = Column(Integer)

    title_id = Column(Integer, ForeignKey('title.id'))


class ProfessionModel(Base):
    __tablename__ = 'profession'

    id = Column(Integer, primary_key=True)
    profession = Column(String(50), nullable=False)

    names = relationship("NameModel", secondary=ProfessionName, backref='profession', cascade='delete,all')


class GenreModel(Base):
    __tablename__ = 'genre'

    id = Column(Integer, primary_key=True)
    genre = Column(String(50), nullable=False)

    titles = relationship("TitleModel", secondary=GenreTitle, backref='genre', cascade='delete,all')
