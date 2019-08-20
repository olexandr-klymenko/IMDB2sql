import csv
import unittest
from os.path import join
from pathlib import Path
from unittest import mock

from src.dataset_parser import DatasetParser
from src.utils import get_config
from tests.utils import get_root_dir, CONFIG_REL_PATH, DATASETS_REL_PATH

CONFIG = get_config(join(get_root_dir(), CONFIG_REL_PATH))
DATASET_DIR = join(get_root_dir(), DATASETS_REL_PATH)
EXPECTED_DATA = {
    "principal.csv": 10,
    "profession.csv": 6,
    "profession_person.csv": 25,
    "rating.csv": 8,
    "film.csv": 8,
    "person_film.csv": 27,
    "genre.csv": 6,
    "genre_film.csv": 15,
    "person.csv": 9,
    "job.csv": 5,
}

EXPECTED_ERRORS = {
    "film": [
        {
            "tconst": "tt0000009",
            "titleType": "movie",
            "primaryTitle": "Miss Jerry",
            "originalTitle": "Miss Jerry",
            "isAdult": "0",
            "startYear": "1894",
            "endYear": "\\N",
            "runtimeMinutes": "Romance",
        }
    ]
}


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
        for path in Path(DATASET_DIR).glob("*.csv"):
            path.unlink()

# TODO: Cover all the rest of cases with different args
# TODO: Increase dataset size in several times
# TODO: Fix cleanup
