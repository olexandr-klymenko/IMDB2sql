from os import getcwd
from os.path import join

from flask import Flask
from flask_graphql import GraphQLView

from src.models import db
from src.schema import schema
from src.utils import get_config

CONFIG = get_config(join(getcwd(), "config", "config.yml"))


def create_app(config=CONFIG):
    app = Flask(__name__)
    app.debug = True
    app.config["SQLALCHEMY_DATABASE_URI"] = config["default_database_uri"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True
    db.init_app(app)

    app.add_url_rule(
        "/graphql",
        view_func=GraphQLView.as_view("graphql", schema=schema, graphiql=True),
    )
    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
