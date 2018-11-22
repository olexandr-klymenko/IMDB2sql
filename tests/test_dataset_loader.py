import unittest
from os import getcwd
from os.path import join, isfile, abspath, pardir
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from typing import List
from unittest import mock

from src import models
from src.dataset_loader import DatasetLoader
from src.dataset_parser import DatasetParser
from src.utils import get_config

DATASET_DIR = join(getcwd(), 'tests', 'datasets')
CONFIG_PATH = join(getcwd(), 'config', 'config.yml')
if not isfile(CONFIG_PATH):
    CONFIG_PATH = abspath(abspath(join(getcwd(), pardir, 'config', 'config.yml')))
    DATASET_DIR = abspath(join(getcwd(), pardir, 'tests', 'datasets'))

CONFIG = get_config(CONFIG_PATH)


class TestDataSetLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cmd_args = mock
        cmd_args.debug = False
        cmd_args.dburi = "postgresql+psycopg2://postgres@127.0.0.1:5434/postgres"
        cmd_args.root = DATASET_DIR
        cmd_args.resume = None
        cmd_args.one = False
        cmd_args.quiet = True

        cls.dataset_parser = DatasetParser(cmd_args, CONFIG)
        cls.dataset_loader = DatasetLoader(cmd_args, CONFIG)
        cls.dataset_parser.parse_dataset()
        cls.dataset_loader.db_init()
        cls.dataset_loader.load_dataset()
        cls.session = sessionmaker(bind=cls.dataset_loader.engine)()

    @classmethod
    def tearDownClass(cls):
        for path in Path(DATASET_DIR).glob('*.csv'):
            path.unlink()
        cls.dataset_loader.clean_up()

    def test_names(self):
        name_model: models.Name = self.session.query(models.Name).filter(models.Name.id == 9).all()[0]
        self.assertEqual(name_model.primary_profession, 'actor,producer,soundtrack')
        self.assertEqual(
            set([title.id for title in name_model.titles]), {1, 2, 3, 4}
        )

    def test_titles(self):
        title_model: models.Title = self.session.query(models.Title).filter(models.Title.id == 2).all()[0]
        self.assertEqual(title_model.original_title, 'Le clown et ses chiens')
        self.assertEqual(
            set(name.id for name in title_model.names), {1, 6, 9}
        )

    def test_principals(self):
        query: List[models.Principals] = self.session.query(
            models.Principals
        ).filter(models.Principals.title_id == 1).all()
        self.assertEqual(len(query), 4)
        for principal in query:
            self.assertIn(principal.category, principal.name.primary_profession.split(','))

    def test_ratings(self):
        query: List[models.Ratings] = self.session.query(
            models.Ratings
        ).filter(models.Ratings.title_id == 1).all()
        self.assertEqual(len(query), 1)
        self.assertEqual(query[0].average_rating, 5.8)
        self.assertEqual(query[0].num_votes, 1396)
        self.assertEqual(query[0].title.id, 1)
