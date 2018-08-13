import unittest
from os.path import join, dirname

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base, Name, Title
from tests.utils import parse_names, parse_titles

DATASETS_DIR = 'datasets'
NAMES_DATASET = 'name.basics.tsv'
TITLES_DATASET = 'title.basics.tsv'


class TestModels(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite:///:memory:', echo=True)
        Base.metadata.create_all(bind=engine)

        session = sessionmaker(bind=engine)
        self.session = session()
        data_sets_root = join(dirname(__file__), DATASETS_DIR)

        self._insert_dataset(parse_titles(join(data_sets_root, TITLES_DATASET)))
        self._insert_dataset(parse_names(join(data_sets_root, NAMES_DATASET)))

        self.session.commit()

    def _insert_dataset(self, dataset_iter):
        for line in dataset_iter:
            self.session.add(line)

    def tearDown(self):
        self.session.close()

    def test_names(self):
        query = self.session.query(Name).filter(Name.primaryName == 'Richard Burton').all()
        self.assertEqual(query[0].primaryProfession, 'actor,producer,soundtrack')

    def test_titles(self):
        query = self.session.query(Title).filter(Title.id == 'tt0000006').all()
        self.assertEqual(query[0].startYear, 1894)
