import unittest
from os.path import join, dirname
from typing import List, Tuple

import src.models as models
from src.dal import ImdbDal
from src.constants import DEFAULT_BATCH_SIZE

DATASETS_DIR = 'datasets'
TITLES_DATASET = 'title.basics.tsv'
INVALID_TITLES_DATASET = 'invalid_title.basics.tsv'
NAMES_DATASET = 'name.basics.tsv'
PRINCIPALS_DATASET = 'title.principals.tsv'
RATINGS_DATASET = 'title.ratings.tsv'


class BaseTestDAL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        (titles_path,
         names_path,
         principals_path,
         ratings_path
         ) = cls._get_dataset_paths()
        cls.dal = ImdbDal(
            root='./datasets',
            resume=None,
            batch_size=DEFAULT_BATCH_SIZE,
            dataset_paths=[
                ('title', titles_path),
                ('name', names_path),
                ('principals', principals_path),
                ('ratings', ratings_path)
            ]
        )
        cls.dal.db_init('sqlite:///:memory:')

    @staticmethod
    def _get_dataset_paths() -> Tuple[str, str, str, str]:
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
        ratings_path = join(data_sets_root, RATINGS_DATASET)
        return titles_path, names_path, principals_path, ratings_path

    def test_names(self):
        query = self.dal.session.query(models.Name).filter(models.Name.id == 9).all()
        self.assertEqual(query[0].primaryProfession, 'actor,producer,soundtrack')
        self.assertEqual(
            set([title.id for title in query[0].titles]), {1, 2, 3, 4}
        )

    def test_titles(self):
        query: List[models.Title] = self.dal.session.query(models.Title).filter(models.Title.id == 2).all()
        self.assertEqual(query[0].originalTitle, 'Le clown et ses chiens')
        self.assertEqual(
            set(name.id for name in query[0].names), {1, 6, 9}
        )

    def test_principals(self):
        query: List[models.Principals] = self.dal.session.query(
            models.Principals
        ).filter(models.Principals.title_id == 1).all()
        self.assertEqual(len(query), 4)
        for principal in query:
            self.assertIn(principal.category, principal.name.primaryProfession.split(','))

    def test_ratings(self):
        query: List[models.Ratings] = self.dal.session.query(
            models.Ratings
        ).filter(models.Ratings.title_id == 1).all()
        self.assertEqual(len(query), 1)
        self.assertEqual(query[0].averageRating, 5.8)
        self.assertEqual(query[0].numVotes, 1396)


class TestDALWithInvalidDataSet(BaseTestDAL):

    @staticmethod
    def _get_dataset_paths():
        data_sets_root = join(dirname(__file__), DATASETS_DIR)
        titles_path = join(data_sets_root, INVALID_TITLES_DATASET)
        names_path = join(data_sets_root, NAMES_DATASET)
        principals_path = join(data_sets_root, PRINCIPALS_DATASET)
        ratings_path = join(data_sets_root, RATINGS_DATASET)
        return titles_path, names_path, principals_path, ratings_path

    def test_titles(self):
        query: List[models.Title] = self.dal.session.query(
            models.Title
        ).filter(models.Title.id.in_([5, 9])).all()
        self.assertEqual(len(query), 0)


# TODO: Cover all the rest of cases with different args
