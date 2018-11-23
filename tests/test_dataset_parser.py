import csv
import unittest
from os.path import join
from pathlib import Path
from unittest import mock

from src.dataset_parser import DatasetParser
from src.utils import get_config
from tests.utils import get_root_dir, CONFIG_REL_PATH, DATASET_REL_PATH

CONFIG = get_config(join(get_root_dir(), CONFIG_REL_PATH))
DATASET_DIR = join(get_root_dir(), DATASET_REL_PATH)
EXPECTED_DATA = {
    'principal.csv': 10,
    'profession.csv': 6,
    'profession_name.csv': 25,
    'rating.csv': 8,
    'title.csv': 8,
    'name_title.csv': 27,
    'genre.csv': 6,
    'genre_title.csv': 15,
    'name.csv': 9
}

EXPECTED_ERRORS = {'title': [
    {'tconst': 'tt0000009', 'titleType': 'movie', 'primaryTitle': 'Miss Jerry', 'originalTitle': 'Miss Jerry',
     'isAdult': '0', 'startYear': '1894', 'endYear': '\\N', 'runtimeMinutes': 'Romance'}],
    'rating': [{'tconst': 'tt0000009', 'averageRating': '5.5', 'numVotes': '67'}]}


class TestDataSetParser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cmd_args = mock
        cmd_args.debug = False
        cmd_args.root = DATASET_DIR
        cmd_args.quiet = True

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

# TODO: Cover all the rest of cases with different args
# TODO: Increase dataset size in several times
