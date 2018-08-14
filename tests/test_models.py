import unittest
from os.path import join, dirname
from typing import Iterable, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base, Name, Title
from tests.utils import parse_names, parse_titles

DATASETS_DIR = 'datasets'
NAMES_DATASET = 'name.basics.tsv'
TITLES_DATASET = 'title.basics.tsv'


class TestModels(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        engine = create_engine('sqlite:///:memory:', echo=True)
        Base.metadata.create_all(bind=engine)

        session = sessionmaker(bind=engine)
        cls.session = session()

    @classmethod
    def tearDownClass(cls):
        cls.session.close()

    def setUp(self):
        data_sets_root = join(dirname(__file__), DATASETS_DIR)

        self._insert_dataset(parse_titles(join(data_sets_root, TITLES_DATASET)))
        self._insert_dataset(parse_names(join(data_sets_root, NAMES_DATASET), self._get_titles))

        self.session.commit()

    def tearDown(self):
        self.session.query(Name).delete()
        self.session.query(Title).delete()
        self.session.commit()

    def _insert_dataset(self, dataset_iter):
        for line in dataset_iter:
            self.session.add(line)

    def _get_titles(self, title_ids: Iterable):
        query = self.session.query(Title)
        query = query.filter(Title.id.in_(title_ids))
        return query.all()

    def test_names(self):
        query = self.session.query(Name).filter(Name.id == 'nm0000009').all()
        self.assertEqual(query[0].primaryProfession, 'actor,producer,soundtrack')
        self.assertEqual(
            set([title.id for title in query[0].titles]), set('tt0000001,tt0000002,tt0000003,tt0000004'.split(','))
        )

    def test_titles(self):
        query: List[Title] = self.session.query(Title).filter(Title.id == 'tt0000002').all()
        self.assertEqual(query[0].originalTitle, 'Le clown et ses chiens')
        self.assertEqual(
            set(name.id for name in query[0].names), {'nm0000001', 'nm0000006', 'nm0000009'}
        )
