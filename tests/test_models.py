from os.path import join, dirname
import unittest
from typing import List

import src.models as models
from src.dal import ImdbDal

DATASETS_DIR = 'datasets'
TITLES_DATASET = 'title.basics.tsv'
NAMES_DATASET = 'name.basics.tsv'


class TestModels(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_sets_root = join(dirname(__file__), DATASETS_DIR)
        titles_path = join(data_sets_root, TITLES_DATASET)
        names_path = join(data_sets_root, NAMES_DATASET)
        cls.dal = ImdbDal(titles_path=titles_path, names_path=names_path)
        cls.dal.db_init()

    @classmethod
    def tearDownClass(cls):
        cls.dal.session.close()

    def setUp(self):
        self.dal.parse_data_sets()

    def tearDown(self):
        self.dal.clean_up()

    def test_names(self):
        query = self.dal.session.query(models.Name).filter(models.Name.id == 'nm0000009').all()
        self.assertEqual(query[0].primaryProfession, 'actor,producer,soundtrack')
        self.assertEqual(
            set([title.id for title in query[0].titles]), set('tt0000001,tt0000002,tt0000003,tt0000004'.split(','))
        )

    def test_titles(self):
        query: List[models.Title] = self.dal.session.query(models.Title).filter(models.Title.id == 'tt0000002').all()
        self.assertEqual(query[0].originalTitle, 'Le clown et ses chiens')
        self.assertEqual(
            set(name.id for name in query[0].names), {'nm0000001', 'nm0000006', 'nm0000009'}
        )
