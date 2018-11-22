import csv
import unittest
from os import getcwd
from os.path import join, isfile, abspath, pardir
from pathlib import Path
from unittest import mock

from src.dataset_parser import DatasetParser
from src.utils import get_config

DATASET_DIR = join(getcwd(), 'tests', 'datasets')
CONFIG_PATH = join(getcwd(), 'config', 'config.yml')
if not isfile(CONFIG_PATH):
    CONFIG_PATH = abspath(abspath(join(getcwd(), pardir, 'config', 'config.yml')))
    DATASET_DIR = abspath(join(getcwd(), pardir, 'tests', 'datasets'))

CONFIG = get_config(CONFIG_PATH)

EXPECTED_DATA = {
    'principals.csv': 10,
    'ratings.csv': 8,
    'title.csv': 8,
    'name_title.csv': 27,
    'name.csv': 9
}

EXPECTED_ERRORS = {'title': [
    {'tconst': 'tt0000009', 'titleType': 'movie', 'primaryTitle': 'Miss Jerry', 'originalTitle': 'Miss Jerry',
     'isAdult': '0', 'startYear': '1894', 'endYear': '\\N', 'runtimeMinutes': 'Romance'}],
    'ratings': [{'tconst': 'tt0000009', 'averageRating': '5.5', 'numVotes': '67'}]}


class TestDataSetParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cmd_args = mock
        cmd_args.debug = False
        cmd_args.root = DATASET_DIR
        cls.dataset_parser = DatasetParser(cmd_args, CONFIG)

    def setUp(self):
        self.dataset_parser.parse_dataset()

    def tearDown(self):
        for path in Path(DATASET_DIR).glob('*.csv'):
            path.unlink()

    def test_parsed_dataset(self):
        actual_dict = {}
        for path in Path(DATASET_DIR).glob('*.csv'):
            with path.open() as f:
                csv_reader = csv.reader(f)
                content = [line for line in csv_reader]
                actual_dict[path.name] = len(content)
        self.assertDictEqual(actual_dict, EXPECTED_DATA)
        self.assertDictEqual(dict(self.dataset_parser.errors), EXPECTED_ERRORS)

# TODO: Fix tests
# TODO: Add test case for DatasetLoader
# TODO: Cover all the rest of cases with different args
# TODO: Increase dataset size in several times
