import graphene
from graphene_sqlalchemy import SQLAlchemyObjectType
from sqlalchemy.sql.functions import count

import src.models as models

QUERY_LIMIT = 50


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
    common_titles = graphene.List(lambda: TitleType, names=graphene.List(graphene.String))
    name = graphene.List(lambda: NameType, id=graphene.ID())
    names = graphene.List(lambda: NameType,
                          search=graphene.String(),
                          profession=graphene.String(),
                          limit=graphene.Int()
                          )
    common_names = graphene.List(lambda: NameType, titles=graphene.List(graphene.String))
    principals = graphene.List(lambda: PrincipalType, limit=graphene.Int())
    ratings = graphene.List(lambda: RatingType, limit=graphene.Int())
    genres = graphene.List(GenreType, search=graphene.String())
    professions = graphene.List(ProfessionType, search=graphene.String())

    def resolve_title(self, info, id):
        query = TitleType.get_query(info)
        return query.filter(models.TitleModel.id == id)

    def resolve_titles(self, info, search: str=None, genre: str=None, limit=QUERY_LIMIT):
        query = TitleType.get_query(info)
        return query.join(
            models.GenreTitle
        ).join(
            models.GenreModel
        ).filter(
            models.TitleModel.primary_title.ilike(search) if search else True
        ).filter(
            models.GenreModel.genre == genre if genre else True
        ).limit(limit)

    def resolve_common_titles(self, info, names):
        name_query = NameType.get_query(info)
        name_ids = [n.id for n in name_query.filter(models.NameModel.primary_name.in_(names)).all()]
        session = info.context['session']
        title_ids = session.query(
            models.TitleModel.id
        ).join(
            models.NameTitle
        ).filter(
            models.NameTitle.c.name_id.in_(name_ids)
        ).group_by(
            models.TitleModel.id
        ).having(
            count(models.TitleModel.id) == len(names)
        )
        return TitleType.get_query(info).filter(models.TitleModel.id.in_(title_ids))

    def resolve_name(self, info, id):
        query = NameType.get_query(info)
        return query.filter(models.NameModel.id == id)

    def resolve_names(self, info, search: str=None, profession=None, limit=QUERY_LIMIT):
        query = NameType.get_query(info)
        return query.join(
            models.ProfessionName
        ).join(
            models.ProfessionModel
        ).filter(
            models.NameModel.primary_name.ilike(search) if search else True
        ).filter(
            models.ProfessionModel.profession == profession if profession else True
        ).limit(limit)

    def resolve_common_names(self, info, titles):
        title_query = TitleType.get_query(info)
        title_ids = [t.id for t in title_query.filter(models.TitleModel.primary_title.in_(titles)).all()]
        session = info.context['session']
        name_ids = session.query(
            models.NameModel.id
        ).join(
            models.NameTitle
        ).filter(
            models.NameTitle.c.title_id.in_(title_ids)
        ).group_by(
            models.NameModel.id
        ).having(
            count(models.NameModel.id) == len(titles)
        )
        return NameType.get_query(info).filter(models.NameModel.id.in_(name_ids))

    def resolve_principals(self, info, limit=QUERY_LIMIT):
        query = PrincipalType.get_query(info)
        return query.limit(limit)

    def resolve_ratings(self, info, limit=QUERY_LIMIT):
        query = RatingType.get_query(info)
        return query.limit(limit)

    def resolve_genres(self, info, search: str=None):
        query = GenreType.get_query(info)
        return query.filter(models.GenreModel.genre.ilike(search) if search else True)

    def resolve_professions(self, info, search: str=None):
        query = ProfessionType.get_query(info)
        return query.filter(models.ProfessionModel.profession.ilike(search) if search else True)


schema = graphene.Schema(query=Query)
