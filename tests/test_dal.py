import unittest
from os.path import join, dirname
from typing import List

import src.models as models
from src.dal import ImdbDal

DATASETS_DIR = 'datasets'
TITLES_DATASET = 'title.basics.tsv'
INVALID_TITLES_DATASET = 'invalid_title.basics.tsv'
NAMES_DATASET = 'name.basics.tsv'
PRINCIPALS_DATASET = 'title.principals.tsv'


class BaseTestDAL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        titles_path, names_path, principals_path = cls._get_dataset_paths()
        cls.dal = ImdbDal(titles_path=titles_path, names_path=names_path, principals_path=principals_path)
        cls.dal.db_init()

    @staticmethod
    def _get_dataset_paths():
        raise NotImplementedError

    @classmethod
    def tearDownClass(cls):
        cls.dal.session.close()

    def setUp(self):
        self.dal.parse_data_sets()

    def tearDown(self):
        self.dal.clean_up()


class TestDAL(BaseTestDAL):

    @staticmethod
    def _get_dataset_paths():
        data_sets_root = join(dirname(__file__), DATASETS_DIR)
        titles_path = join(data_sets_root, TITLES_DATASET)
        names_path = join(data_sets_root, NAMES_DATASET)
        principals_path = join(data_sets_root, PRINCIPALS_DATASET)
        return titles_path, names_path, principals_path

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

    def test_principals(self):
        query: List[models.Principals] = self.dal.session.query(
            models.Principals
        ).filter(models.Principals.title_id == 'tt0000001').all()
        self.assertEqual(len(query), 4)
        for principal in query:
            self.assertIn(principal.category, principal.name.primaryProfession.split(','))


class TestDALWithInvalidDataSet(BaseTestDAL):

    @staticmethod
    def _get_dataset_paths():
        data_sets_root = join(dirname(__file__), DATASETS_DIR)
        titles_path = join(data_sets_root, INVALID_TITLES_DATASET)
        names_path = join(data_sets_root, NAMES_DATASET)
        principals_path = join(data_sets_root, PRINCIPALS_DATASET)
        return titles_path, names_path, principals_path

    def test_titles(self):
        query: List[models.Title] = self.dal.session.query(
            models.Title
        ).filter(models.Title.id.in_(['tt0000005', 'tt0000009'])).all()
        self.assertEqual(len(query), 0)