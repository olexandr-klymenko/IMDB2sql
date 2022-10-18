from os import getcwd
from pathlib import Path

from flask import Flask, render_template
from flask_graphql import GraphQLView

from src.models import db
from src.schema import schema
from src.utils import get_config

CONFIG = get_config(Path(Path(getcwd()) / "config" / "config.yml"))


def create_app(config=CONFIG):
    app = Flask(__name__, template_folder="src/templates", static_folder="src/static")
    app.debug = True
    app.config["SQLALCHEMY_DATABASE_URI"] = config["default_database_uri"]
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = True
    db.init_app(app)

    app.add_url_rule(
        "/graphql",
        view_func=GraphQLView.as_view("graphql", schema=schema, graphiql=True),
    )

    @app.route("/")
    def index():
        return render_template("index.html")

    return app


if __name__ == "__main__":
    app = create_app()
    app.run()
