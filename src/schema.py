import graphene
from graphene import relay
from graphene_sqlalchemy import SQLAlchemyObjectType

import src.models as models


class TitleType(SQLAlchemyObjectType):
    class Meta:
        model = models.TitleModel
        interfaces = (relay.Node,)


class NameType(SQLAlchemyObjectType):
    class Meta:
        model = models.NameModel
        interfaces = (relay.Node,)


class Query(graphene.ObjectType):
    node = relay.Node.Field()

    titles = graphene.List(TitleType)
    names = graphene.List(NameType)

    def resolve_titles(self, info):
        query = TitleType.get_query(info)
        return query.all()

    def resolve_names(self, info):
        query = NameType.get_query(info)
        return query.all()


schema = graphene.Schema(query=Query)
