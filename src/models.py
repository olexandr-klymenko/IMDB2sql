from flask_sqlalchemy import SQLAlchemy


db = SQLAlchemy()


PersonFilm = db.Table(
    "person_film",
    db.Column("person_id", db.Integer, db.ForeignKey("person.id")),
    db.Column("film_id", db.Integer, db.ForeignKey("film.id")),
)

ProfessionPerson = db.Table(
    "profession_person",
    db.Column("profession_id", db.Integer, db.ForeignKey("profession.id")),
    db.Column("person_id", db.Integer, db.ForeignKey("person.id")),
)

GenreFilm = db.Table(
    "genre_film",
    db.Column("genre_id", db.Integer, db.ForeignKey("genre.id")),
    db.Column("film_id", db.Integer, db.ForeignKey("film.id")),
)


class FilmModel(db.Model):
    __tablename__ = "film"

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(450), index=True)
    is_adult = db.Column(db.Boolean)
    start_year = db.Column(db.Integer)
    runtime_minutes = db.Column(db.Integer)

    persons = db.relationship(
        "PersonModel",
        secondary=PersonFilm,
        cascade="delete,all",
        back_populates="films",
    )
    principals = db.relationship("PrincipalModel", backref="film", cascade="delete,all")
    rating = db.relationship(
        "RatingModel", backref="film", uselist=False, cascade="delete,all"
    )
    genres = db.relationship(
        "GenreModel", secondary=GenreFilm, cascade="delete,all", back_populates="films"
    )


class PersonModel(db.Model):
    __tablename__ = "person"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), index=True)
    birth_year = db.Column(db.Integer)
    death_year = db.Column(db.Integer, nullable=True)

    films = db.relationship(
        "FilmModel",
        secondary=PersonFilm,
        cascade="delete,all",
        back_populates="persons",
    )
    professions = db.relationship(
        "ProfessionModel",
        secondary=ProfessionPerson,
        cascade="delete,all",
        back_populates="persons",
    )
    principals = db.relationship(
        "PrincipalModel", backref="person", cascade="delete,all"
    )


class JobModel(db.Model):
    __tablename__ = "job"

    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(20))

    principals = db.relationship(
        "PrincipalModel", cascade="delete,all", back_populates="job"
    )


class PrincipalModel(db.Model):
    __tablename__ = "principal"

    id = db.Column(db.Integer, primary_key=True)

    film_id = db.Column(db.Integer, db.ForeignKey("film.id"))
    person_id = db.Column(db.Integer, db.ForeignKey("person.id"))
    job_id = db.Column(db.Integer, db.ForeignKey("job.id"))

    job = db.relationship("JobModel", uselist=False, back_populates="principals")


class RatingModel(db.Model):
    __tablename__ = "rating"

    id = db.Column(db.Integer, primary_key=True)
    average_rating = db.Column(db.Float)
    num_votes = db.Column(db.Integer)

    film_id = db.Column(db.Integer, db.ForeignKey("film.id"))


class ProfessionModel(db.Model):
    __tablename__ = "profession"

    id = db.Column(db.Integer, primary_key=True)
    profession = db.Column(db.String(50), nullable=False)

    persons = db.relationship(
        "PersonModel",
        secondary=ProfessionPerson,
        cascade="delete,all",
        back_populates="professions",
    )


class GenreModel(db.Model):
    __tablename__ = "genre"

    id = db.Column(db.Integer, primary_key=True)
    genre = db.Column(db.String(50), nullable=False)

    films = db.relationship(
        "FilmModel", secondary=GenreFilm, cascade="delete,all", back_populates="genres"
    )
