from os import getcwd
from os.path import join

from flask import Flask
from flask_graphql import GraphQLView
from sqlalchemy import create_engine

from src.schema import schema
from src.utils import get_config

CONFIG = get_config(join(getcwd(), 'config', 'config.yml'))

app = Flask(__name__)
app.debug = True


app.add_url_rule('/graphql', view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True))


if __name__ == '__main__':
    create_engine(CONFIG['default_database_uri'])
    app.run()
