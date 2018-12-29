from sqlalchemy import Column, String, Integer, Float, Boolean, ForeignKey, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base(metadata=MetaData())

PersonFilm = Table('person_film',
                   Base.metadata,
                   Column('person_id', Integer, ForeignKey('person.id')),
                   Column('film_id', Integer, ForeignKey('film.id'))
                   )

ProfessionPerson = Table('profession_person',
                         Base.metadata,
                         Column('profession_id', Integer, ForeignKey('profession.id')),
                         Column('person_id', Integer, ForeignKey('person.id'))
                         )

GenreFilm = Table('genre_film',
                  Base.metadata,
                  Column('genre_id', Integer, ForeignKey('genre.id')),
                  Column('film_id', Integer, ForeignKey('film.id')),
                  )


class FilmModel(Base):
    __tablename__ = 'film'

    id = Column(Integer, primary_key=True)
    title = Column(String(450), index=True)
    is_adult = Column(Boolean)
    start_year = Column(Integer)
    runtime_minutes = Column(Integer)

    persons = relationship("PersonModel", secondary=PersonFilm, backref='film', cascade='delete,all')
    principals = relationship("PrincipalModel", backref='film', cascade='delete,all')
    rating = relationship("RatingModel", backref='film', uselist=False, cascade='delete,all')
    genres = relationship("GenreModel", secondary=GenreFilm, backref='film', cascade='delete,all')


class PersonModel(Base):
    __tablename__ = 'person'

    id = Column(Integer, primary_key=True)
    name = Column(String(120), index=True)
    birth_year = Column(Integer)
    death_year = Column(Integer, nullable=True)

    films = relationship("FilmModel", secondary=PersonFilm, backref='person', cascade='delete,all')
    professions = relationship("ProfessionModel", secondary=ProfessionPerson, backref='person', cascade='delete,all')
    principals = relationship("PrincipalModel", backref='person', cascade='delete,all')


class JobModel(Base):
    __tablename__ = 'job'

    id = Column(Integer, primary_key=True)
    job = Column(String(20))

    principals = relationship("PrincipalModel", cascade='delete,all')


class PrincipalModel(Base):
    __tablename__ = 'principal'

    id = Column(Integer, primary_key=True)

    film_id = Column(Integer, ForeignKey('film.id'))
    person_id = Column(Integer, ForeignKey('person.id'))
    job_id = Column(Integer, ForeignKey('job.id'))

    job = relationship("JobModel", uselist=False)


class RatingModel(Base):
    __tablename__ = 'rating'

    id = Column(Integer, primary_key=True)
    average_rating = Column(Float)
    num_votes = Column(Integer)

    film_id = Column(Integer, ForeignKey('film.id'))


class ProfessionModel(Base):
    __tablename__ = 'profession'

    id = Column(Integer, primary_key=True)
    profession = Column(String(50), nullable=False)

    persons = relationship("PersonModel", secondary=ProfessionPerson, backref='profession', cascade='delete,all')


class GenreModel(Base):
    __tablename__ = 'genre'

    id = Column(Integer, primary_key=True)
    genre = Column(String(50), nullable=False)

    films = relationship("FilmModel", secondary=GenreFilm, backref='genre', cascade='delete,all')
