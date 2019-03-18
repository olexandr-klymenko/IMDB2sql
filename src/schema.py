import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType
from sqlalchemy.sql.functions import count

import src.models as models

QUERY_LIMIT = 50


class ActiveSQLAlchemyObjectType(SQLAlchemyObjectType):
    class Meta:
        abstract = True


class FilmType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.FilmModel

    persons = graphene.List(
        lambda: PersonType, search=graphene.String(), profession=graphene.String()
    )

    def resolve_persons(self, info, search: str = None, profession=None):
        query = PersonType.get_query(info)
        return (
            query.join(models.ProfessionPerson)
            .join(models.ProfessionModel)
            .join(models.PersonFilm)
            .join(models.FilmModel)
            .filter(models.PersonModel.name.ilike(search) if search else True)
            .filter(
                models.ProfessionModel.profession == profession if profession else True
            )
            .filter(models.FilmModel.id == self.id)
        )


class PersonType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.PersonModel

    films = graphene.List(
        lambda: FilmType,
        search=graphene.String(),
        genre=graphene.String(),
        period=graphene.List(graphene.Int),
    )

    def resolve_films(self, info, search: str = None, genre: str = None, period=None):
        query = FilmType.get_query(info)
        return (
            query.join(models.GenreFilm)
            .join(models.GenreModel)
            .join(models.PersonFilm)
            .join(models.PersonModel)
            .filter(models.FilmModel.title.ilike(search) if search else True)
            .filter(models.GenreModel.genre == genre if genre else True)
            .filter(
                models.FilmModel.start_year.between(period[0], period[1])
                if period
                else True
            )
            .filter(models.PersonModel.id == self.id)
        )


class PrincipalType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.PrincipalModel

    job = graphene.String()
    name = graphene.String()
    title = graphene.String()

    person: models.PersonModel = PersonType()
    film: models.FilmModel = FilmType()

    def resolve_name(self, _):
        return self.person.name

    def resolve_title(self, _):
        return self.film.title

    def resolve_job(self, _):
        return self.job.job


class JobType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.JobModel


class RatingType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.RatingModel


class GenreType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.GenreModel


class ProfessionType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.ProfessionModel


class Query(graphene.ObjectType):
    film = graphene.List(lambda: FilmType, id=graphene.ID())
    films = graphene.List(
        lambda: FilmType,
        search=graphene.String(),
        genre=graphene.String(),
        period=graphene.List(graphene.Int),
        limit=graphene.Int(),
    )
    common_films = graphene.List(lambda: FilmType, names=graphene.List(graphene.String))
    person = graphene.List(lambda: PersonType, id=graphene.ID())
    persons = graphene.List(
        lambda: PersonType,
        search=graphene.String(),
        profession=graphene.String(),
        limit=graphene.Int(),
    )
    common_persons = graphene.List(
        lambda: PersonType, titles=graphene.List(graphene.String)
    )
    principals = graphene.List(
        lambda: PrincipalType,
        person_id=graphene.ID(),
        film_id=graphene.ID(),
        job=graphene.String(),
        limit=graphene.Int(),
    )
    ratings = graphene.List(lambda: RatingType, limit=graphene.Int())
    genres = graphene.List(GenreType, search=graphene.String())
    professions = graphene.List(ProfessionType, search=graphene.String())

    jobs = graphene.List(lambda: JobType)

    def resolve_film(self, info, id):
        query = FilmType.get_query(info)
        return query.filter(models.FilmModel.id == id)

    def resolve_films(
        self,
        info,
        search: str = None,
        genre: str = None,
        period=None,
        limit=QUERY_LIMIT,
    ):
        query = FilmType.get_query(info)
        return (
            query.join(models.GenreFilm)
            .join(models.GenreModel)
            .filter(models.FilmModel.title.ilike(search) if search else True)
            .filter(models.GenreModel.genre == genre if genre else True)
            .filter(
                models.FilmModel.start_year.between(period[0], period[1])
                if period
                else True
            )
            .limit(limit)
        )

    def resolve_common_films(self, info, names):
        name_query = PersonType.get_query(info)
        person_ids = [
            n.id for n in name_query.filter(models.PersonModel.name.in_(names)).all()
        ]
        session = info.context["session"]
        film_ids = (
            session.query(models.FilmModel.id)
            .join(models.PersonFilm)
            .filter(models.PersonFilm.c.person_id.in_(person_ids))
            .group_by(models.FilmModel.id)
            .having(count(models.FilmModel.id) == len(names))
        )
        return FilmType.get_query(info).filter(models.FilmModel.id.in_(film_ids))

    def resolve_person(self, info, id):
        query = PersonType.get_query(info)
        return query.filter(models.PersonModel.id == id)

    def resolve_persons(
        self, info, search: str = None, profession=None, limit=QUERY_LIMIT
    ):
        query = PersonType.get_query(info)
        return (
            query.join(models.ProfessionPerson)
            .join(models.ProfessionModel)
            .filter(models.PersonModel.name.ilike(search) if search else True)
            .filter(
                models.ProfessionModel.profession == profession if profession else True
            )
            .limit(limit)
        )

    def resolve_common_persons(self, info, titles):
        title_query = FilmType.get_query(info)
        film_ids = [
            t.id for t in title_query.filter(models.FilmModel.title.in_(titles)).all()
        ]
        session = info.context["session"]
        person_ids = (
            session.query(models.PersonModel.id)
            .join(models.PersonFilm)
            .filter(models.PersonFilm.c.film_id.in_(film_ids))
            .group_by(models.PersonModel.id)
            .having(count(models.PersonModel.id) == len(titles))
        )
        return PersonType.get_query(info).filter(models.PersonModel.id.in_(person_ids))

    def resolve_principals(
        self, info, person_id=None, film_id=None, job=None, limit=QUERY_LIMIT
    ):
        query = PrincipalType.get_query(info)
        return (
            query.filter(
                models.PrincipalModel.person_id == person_id if person_id else True
            )
            .filter(models.PrincipalModel.film_id == film_id if film_id else True)
            .join(models.JobModel)
            .filter(models.JobModel.job == job if job else True)
            .limit(limit)
        )

    def resolve_ratings(self, info, limit=QUERY_LIMIT):
        query = RatingType.get_query(info)
        return query.limit(limit)

    def resolve_genres(self, info, search: str = None):
        query = GenreType.get_query(info)
        return query.filter(models.GenreModel.genre.ilike(search) if search else True)

    def resolve_professions(self, info, search: str = None):
        query = ProfessionType.get_query(info)
        return query.filter(
            models.ProfessionModel.profession.ilike(search) if search else True
        )

    def resolve_jobs(self, info):
        return JobType.get_query(info)


schema = graphene.Schema(query=Query)


# TODO: Cover schema with tests
