import unittest
from os.path import join
from pathlib import Path
from sqlalchemy.orm import sessionmaker
from typing import List
from unittest import mock

from src import models
from src.dataset_loader import DatasetLoader
from src.dataset_parser import DatasetParser
from src.utils import get_config
from tests.utils import get_root_dir, CONFIG_REL_PATH, DATASETS_REL_PATH

CONFIG = get_config(join(get_root_dir(), CONFIG_REL_PATH))
DATASET_DIR = join(get_root_dir(), DATASETS_REL_PATH)


class TestDataSetLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cmd_args = mock
        cmd_args.debug = False
        cmd_args.dburi = (
            "postgresql+psycopg2://postgres:example@127.0.0.1:5434/postgres"
        )
        cmd_args.root = DATASET_DIR
        cmd_args.resume = None
        cmd_args.quiet = True

        cls.dataset_parser = DatasetParser(cmd_args, CONFIG)
        cls.dataset_loader = DatasetLoader(cmd_args, CONFIG)
        cls.dataset_parser.parse_dataset()
        cls.dataset_loader.db_init()
        cls.dataset_loader.load_dataset()
        cls.session = sessionmaker(bind=cls.dataset_loader.engine)()

    @classmethod
    def tearDownClass(cls):
        for path in Path(DATASET_DIR).glob("*.csv"):
            path.unlink()
        cls.dataset_loader.clean_up()

    def test_person(self):
        person_model: models.PersonModel = (
            self.session.query(models.PersonModel)
            .filter(models.PersonModel.id == 9)
            .all()[0]
        )
        self.assertSetEqual(
            set([el.profession for el in person_model.professions]),
            {"actor", "producer", "soundtrack"},
        )
        self.assertSetEqual(set([film.id for film in person_model.films]), {1, 2, 3, 4})

    def test_film(self):
        film_model: models.FilmModel = (
            self.session.query(models.FilmModel)
            .filter(models.FilmModel.id == 2)
            .all()[0]
        )
        self.assertEqual(film_model.title, "Le clown et ses chiens")
        self.assertEqual(set(person.id for person in film_model.persons), {1, 6, 9})
        self.assertSetEqual(
            set(genre.genre for genre in film_model.genres), {"Short", "Animation"}
        )

    def test_principals(self):
        query: List[models.PrincipalModel] = (
            self.session.query(models.PrincipalModel)
            .filter(models.PrincipalModel.film_id == 1)
            .all()
        )
        self.assertEqual(len(query), 4)
        for principal in query:
            self.assertIn(
                principal.job.job,
                [el.profession for el in principal.person.professions],
            )

    def test_ratings(self):
        query: List[models.RatingModel] = (
            self.session.query(models.RatingModel)
            .filter(models.RatingModel.film_id == 1)
            .all()
        )
        self.assertEqual(len(query), 1)
        self.assertEqual(query[0].average_rating, 5.8)
        self.assertEqual(query[0].num_votes, 1396)
        self.assertEqual(query[0].film.id, 1)
