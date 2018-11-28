from os import getcwd
from os.path import join

from flask import Flask
from flask_graphql import GraphQLView
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from src.schema import schema
from src.utils import get_config

CONFIG = get_config(join(getcwd(), 'config', 'config.yml'))

app = Flask(__name__)
app.debug = True

engine = create_engine(CONFIG['default_database_uri'])
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

app.add_url_rule('/graphql',
                 view_func=GraphQLView.as_view('graphql',
                                               schema=schema,
                                               graphiql=True,
                                               get_context=lambda: {'session': db_session}))


if __name__ == '__main__':
    app.run()
