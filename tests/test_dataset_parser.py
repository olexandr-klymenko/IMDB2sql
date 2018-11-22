import unittest
from os import getcwd
from os.path import join
from pathlib import Path

from src.dataset_parser import DatasetParser
from src.utils import get_config

DATASET_DIR = join(getcwd(), 'tests', 'datasets')
CONFIG = get_config(join(getcwd(), 'config', 'config.yml'))


class TestDataSetParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset_parser_cls = DatasetParser
        cls.dataset_parser_cls.root = DATASET_DIR
        cls.dataset_parser_cls.debug = '*'
        cls.dataset_parser = cls.dataset_parser_cls(None, CONFIG)

    def setUp(self):
        self.dataset_parser.parse_dataset()

    def tearDown(self):
        for path in Path(DATASET_DIR).glob('*.csv'):
            path.unlink()

    def test_dataset(self):
        pass


# TODO: Fix tests
# TODO: Add test case for DatasetLoader
# TODO: Cover all the rest of cases with different args
# TODO: Increase dataset size in several times
