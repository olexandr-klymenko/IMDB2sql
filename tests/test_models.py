import unittest
from os.path import join, dirname

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models import Base, Name
from tests.utils import parse_names

DATASETS_DIR = 'datasets'
NAMES_DATASET = 'name.basics.tsv'


class TestModels(unittest.TestCase):

    def setUp(self):
        engine = create_engine('sqlite:///:memory:', echo=True)
        Base.metadata.create_all(bind=engine)

        session = sessionmaker(bind=engine)
        self.session = session()
        data_sets_root = join(dirname(__file__), DATASETS_DIR)
        names_data_set_path = join(data_sets_root, NAMES_DATASET)

        for line in parse_names(names_data_set_path):
            self.session.add(line)

        self.session.commit()

    def tearDown(self):
        self.session.close()

    def test_names(self):
        query = self.session.query(Name).filter(Name.primaryName == 'Richard Burton').all()
        self.assertEqual(query[0].primaryProfession, 'actor,producer,soundtrack')
