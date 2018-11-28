import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType
from sqlalchemy import and_

import src.models as models

QUERY_LIMIT = 500


class ActiveSQLAlchemyObjectType(SQLAlchemyObjectType):
    class Meta:
        abstract = True

    @classmethod
    def get_node(cls, info, _id):
        return cls.get_query(
            info
        ).filter(
            and_(
                cls._meta.model.deleted_at == None,
                cls._meta.model.id == _id
            )
        ).first()


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


class Query(graphene.ObjectType):

    titles = graphene.List(lambda: TitleType, primary_title=graphene.String(), limit=graphene.Int())
    names = graphene.List(lambda: NameType, primary_name=graphene.String(), limit=graphene.Int())
    principals = graphene.List(lambda: PrincipalType, limit=graphene.Int())
    ratings = graphene.List(lambda: RatingType, limit=graphene.Int())
    genres = graphene.List(GenreType)

    def resolve_titles(self, info, primary_title=None, limit=QUERY_LIMIT):
        query = TitleType.get_query(info)
        if primary_title is not None:
            return query.filter(models.TitleModel.primary_title.like(f'%{primary_title}%')).limit(limit)
        return query.limit(limit)

    def resolve_names(self, info, primary_name=None, limit=QUERY_LIMIT):
        query = NameType.get_query(info)
        if primary_name:
            return query.filter(models.NameModel.primary_name.like(f'%{primary_name}%')).limit(limit)
        return query.limit(limit)

    def resolve_principals(self, info, limit=QUERY_LIMIT):
        query = PrincipalType.get_query(info)
        return query.limit(limit)

    def resolve_ratings(self, info, limit=QUERY_LIMIT):
        query = RatingType.get_query(info)
        return query.limit(limit)

    def resolve_genres(self, info):
        query = GenreType.get_query(info)
        return query.all()


schema = graphene.Schema(query=Query)
