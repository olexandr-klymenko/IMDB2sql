import unittest
from typing import List

from sqlalchemy.orm import sessionmaker

import src.models as models
from src.constants import DEFAULT_DATABASE_URI
from src.dataset_parser import DatasetParser

DATASETS_DIR = './datasets'


class TestDataSetParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset_parser = DatasetParser(
            root=DATASETS_DIR,
            resume=None,
            dataset_paths=DATASET_PATHS,
            one=False,
            dry_run=False
        )
        cls.dataset_parser.db_init(DEFAULT_DATABASE_URI)

    def setUp(self):
        self.dataset_parser.parse_dataset()
        self.session = sessionmaker(bind=self.dataset_parser.engine)()

    def tearDown(self):
        self.dataset_parser.clean_up()

    def test_names(self):
        name_model: models.Name = self.session.query(models.Name).filter(models.Name.nconst == 9).all()[0]
        self.assertEqual(name_model.primary_profession, 'actor,producer,soundtrack')
        self.assertEqual(
            set([title.id for title in name_model.titles]), {1, 2, 3, 4}
        )
        self.assertEqual(self.dataset_parser.errors, {
            'NameTitle': {'name_id': 7, 'title_id': 18},
            'ratings': {'average_rating': '5.5', 'num_votes': '67', 'title_id': 9}
        })

    def test_titles(self):
        title_model: models.Title = self.session.query(models.Title).filter(models.Title.tconst == 2).all()[0]
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

# TODO: Fix tests
# TODO: Add test case for DatasetLoader
# TODO: Cover all the rest of cases with different args
# TODO: Increase datasets size in several times
