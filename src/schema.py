import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType
from sqlalchemy.sql.functions import count

import src.models as models

QUERY_LIMIT = 500


class ActiveSQLAlchemyObjectType(SQLAlchemyObjectType):
    class Meta:
        abstract = True


class TitleType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.TitleModel


class NameType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.NameModel


class PrincipalType(ActiveSQLAlchemyObjectType):
    class Meta:
        model = models.PrincipalModel


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

    title = graphene.List(lambda: TitleType, id=graphene.ID())
    titles = graphene.List(lambda: TitleType, search=graphene.String(), genre=graphene.String(), limit=graphene.Int())
    common_titles = graphene.List(lambda: graphene.String, names=graphene.List(graphene.String))
    name = graphene.List(lambda: NameType, id=graphene.ID())
    names = graphene.List(lambda: NameType,
                          search=graphene.String(),
                          profession=graphene.String(),
                          limit=graphene.Int()
                          )
    common_names = graphene.List(lambda: graphene.String, titles=graphene.List(graphene.String))
    principals = graphene.List(lambda: PrincipalType, limit=graphene.Int())
    ratings = graphene.List(lambda: RatingType, limit=graphene.Int())
    genres = graphene.List(GenreType)
    professions = graphene.List(ProfessionType)

    def resolve_title(self, info, id):
        query = TitleType.get_query(info)
        return query.filter(models.TitleModel.id == id)

    def resolve_titles(self, info, search: str, genre=None, limit=QUERY_LIMIT):
        query = TitleType.get_query(info)
        return query.filter(
            models.TitleModel.primary_title.ilike(f'%{search}%')
        ).filter(
            models.GenreModel.genre == genre if genre else True
        ).limit(limit)

    def resolve_common_titles(self, info, names):
        name_query = NameType.get_query(info)
        name_ids = [n.id for n in name_query.filter(models.NameModel.primary_name.in_(names)).all()]
        session = info.context['session']
        result = session.query(
            models.TitleModel.primary_title
        ).join(
            models.NameTitle
        ).filter(
            models.NameTitle.c.name_id.in_(name_ids)
        ).group_by(
            models.TitleModel.primary_title
        ).having(
            count(models.TitleModel.primary_title) == len(names)
        )
        return [value for value, in result]

    def resolve_name(self, info, id):
        query = NameType.get_query(info)
        return query.filter(models.NameModel.id == id)

    def resolve_names(self, info, search: str, profession=None, limit=QUERY_LIMIT):
        query = NameType.get_query(info)
        return query.filter(
            models.NameModel.primary_name.ilike(search)
        ).filter(
            models.ProfessionModel.profession == profession if profession else True
        ).limit(limit)

    def resolve_common_names(self, info, titles):
        title_query = TitleType.get_query(info)
        title_ids = [t.id for t in title_query.filter(models.TitleModel.primary_title.in_(titles)).all()]
        session = info.context['session']
        result = session.query(
            models.NameModel.primary_name
        ).join(
            models.NameTitle
        ).filter(
            models.NameTitle.c.title_id.in_(title_ids)
        ).group_by(
            models.NameModel.primary_name
        ).having(
            count(models.NameModel.primary_name) == len(titles)
        )
        return [value for value, in result]

    def resolve_principals(self, info, limit=QUERY_LIMIT):
        query = PrincipalType.get_query(info)
        return query.limit(limit)

    def resolve_ratings(self, info, limit=QUERY_LIMIT):
        query = RatingType.get_query(info)
        return query.limit(limit)

    def resolve_genres(self, info):
        query = GenreType.get_query(info)
        return query.all()

    def resolve_professions(self, info):
        query = ProfessionType.get_query(info)
        return query.all()


schema = graphene.Schema(query=Query)


# TODO: Add mutations
