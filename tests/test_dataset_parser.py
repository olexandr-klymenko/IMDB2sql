import unittest
from sqlalchemy.orm import sessionmaker
from typing import List

import src.models as models
from src.constants import DEFAULT_MAX_MEMORY_FOOTPRINT, DATASET_PATHS
from src.dataset_parser import DatasetParser

DATASETS_DIR = 'datasets'
TITLES_DATASET = 'title.basics.tsv'
NAMES_DATASET = 'name.basics.tsv'
PRINCIPALS_DATASET = 'title.principals.tsv'
RATINGS_DATASET = 'title.ratings.tsv'


class TestDataSetParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset_parser = DatasetParser(
            root='./datasets',
            resume=None,
            max_footprint=DEFAULT_MAX_MEMORY_FOOTPRINT,
            dataset_paths=DATASET_PATHS,
            one=False
        )
        cls.dataset_parser.db_init('sqlite:///:memory:')

    def setUp(self):
        self.dataset_parser.parse_data_sets()
        self.session = sessionmaker(bind=self.dataset_parser.engine)()

    def tearDown(self):
        self.dataset_parser.clean_up()

    def test_names(self):
        name_model: models.Name = self.session.query(models.Name).filter(models.Name.id == 9).all()[0]
        self.assertEqual(name_model.primaryProfession, 'actor,producer,soundtrack')
        self.assertEqual(
            set([title.id for title in name_model.titles]), {1, 2, 3, 4}
        )

    def test_titles(self):
        title_model: models.Title = self.session.query(models.Title).filter(models.Title.id == 2).all()[0]
        self.assertEqual(title_model.originalTitle, 'Le clown et ses chiens')
        self.assertEqual(
            set(name.id for name in title_model.names), {1, 6, 9}
        )

    def test_principals(self):
        query: List[models.Principals] = self.session.query(
            models.Principals
        ).filter(models.Principals.title_id == 1).all()
        self.assertEqual(len(query), 4)
        for principal in query:
            self.assertIn(principal.category, principal.name.primaryProfession.split(','))

    def test_ratings(self):
        query: List[models.Ratings] = self.session.query(
            models.Ratings
        ).filter(models.Ratings.title_id == 1).all()
        self.assertEqual(len(query), 1)
        self.assertEqual(query[0].averageRating, 5.8)
        self.assertEqual(query[0].numVotes, 1396)
        self.assertEqual(query[0].title.id, 1)

# TODO: Cover all the rest of cases with different args
# TODO: Increase datasets size in several times
