import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType
from sqlalchemy import and_

import src.models as models


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

    titles = graphene.List(TitleType)
    names = graphene.List(NameType)
    principals = graphene.List(PrincipalType)
    ratings = graphene.List(RatingType)
    genres = graphene.List(GenreType)

    def resolve_titles(self, info):
        query = TitleType.get_query(info)
        return query.all()

    def resolve_names(self, info):
        query = NameType.get_query(info)
        return query.all()

    def resolve_principals(self, info):
        query = PrincipalType.get_query(info)
        return query.all()

    def resolve_ratings(self, info):
        query = RatingType.get_query(info)
        return query.all()

    def resolve_genres(self, info):
        query = GenreType.get_query(info)
        return query.all()


schema = graphene.Schema(query=Query)
