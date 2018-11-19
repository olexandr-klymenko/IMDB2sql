import csv
import os
import sqlite3
import sys
from collections import namedtuple, defaultdict
from os.path import join, getsize, exists
from typing import Iterator, List, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.models as models
from src.utils import overwrite_upper_line, get_int, get_footprint, get_pretty_int

SQLITE_TYPE = 'sqlite:///'


class DatasetParser:
    def __init__(self, root, resume, max_footprint: int, dataset_paths: List, one: bool, dry_run:bool):
        self.engine = None
        self.metadata = None
        self.resume = resume
        if self.resume is not None:
            idx = [el[0] for el in dataset_paths].index(self.resume)
            self.dataset_paths = dataset_paths[idx:]
        else:
            self.dataset_paths = dataset_paths

        if one:
            self.dataset_paths = [self.dataset_paths[0]]

        self.root = root
        self.max_footprint = max_footprint
        self.dry_run = dry_run
        self.echo = False
        self.db_uri = None
        self.name_title: List[Dict] = []
        self.invalid_ids = defaultdict(set)
        self.title_ids = set()
        self.name_ids = set()

    def db_init(self, db_uri: str):
        if db_uri.endswith('.db') or db_uri.startswith(SQLITE_TYPE):
            sqlite_path = db_uri.split('///')[1]
            if exists(sqlite_path) and self.resume is None:
                os.remove(sqlite_path)
            sqlite3.connect(db_uri.split('///')[1])

        self.db_uri = db_uri or self.db_uri

        self.engine = create_engine(f'{self.db_uri}', echo=self.echo)
        self.metadata = models.Base.metadata
        self.metadata.create_all(bind=self.engine)
        self.metadata.reflect(bind=self.engine)

        if self.resume:
            self._set_foreign_keys()

    def _set_foreign_keys(self):
        session = self._get_session()
        print("Filling title ids collection ...")
        for result in session.query(models.Title.id).all():
            self.title_ids.add(result[0])
        print(f"Size of 'self.title_ids': {sys.getsizeof(self.title_ids)}")

        print("Filling name ids collection ...")
        for result in session.query(models.Name.id).all():
            self.name_ids.add(result[0])
        print(f"Size of 'self.name_ids': {sys.getsizeof(self.name_ids)}")

    def parse_data_sets(self):
        for table_name, dataset_path in self.dataset_paths:
            status_line = f"Parsing '{dataset_path}' into '{table_name}' table ..."
            parse_handler = self._get_parse_handler(table_name)
            dataset = parse_handler(join(self.root, dataset_path))
            self._insert_dataset(dataset, status_line)

        print(f"Invalid dataset ids:{self.invalid_ids}")

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    def _insert_dataset(self, dataset_iter: Iterator, status_line: str):
        conn = self.engine.connect()
        for idx, (statement, data_line, progress) in enumerate(dataset_iter):
            overwrite_upper_line(self._get_status_line(status_line, progress))
            conn.execute(statement, **data_line)

    @staticmethod
    def _get_status_line(status_line, progress):
        return f'{status_line}: {progress:.2f}%\tmemory footprint: {get_pretty_int(get_footprint())}'

    def _is_time_for_commit(self):
        return self.max_footprint < get_footprint()

    def _get_session(self):
        return sessionmaker(bind=self.engine)()

    def _parse_title(self, dataset_path):
        self.clean_table(models.Title)
        statement = models.Title.__table__.insert()
        for data_set_class, progress in self._parse_dataset(dataset_path, 'title'):
            title_id = get_int(getattr(data_set_class, 'tconst'))
            self.title_ids.add(title_id)
            data_line = {
                "id": title_id,
                "title_type": getattr(data_set_class, 'titleType'),
                "primary_title": getattr(data_set_class, 'primaryTitle'),
                "original_title": getattr(data_set_class, 'originalTitle'),
                "is_adult": bool(getattr(data_set_class, 'isAdult')),
                "start_year": self._get_null(getattr(data_set_class, 'startYear')),
                "end_year": self._get_null(getattr(data_set_class, 'endYear')),
                "runtime_minutes": self._get_null(getattr(data_set_class, 'runtimeMinutes')),
                "genres": getattr(data_set_class, 'genres'),
            }

            yield statement, data_line, progress

    def _parse_name(self, dataset_path):
        self.clean_table(models.Name)
        statement = models.Name.__table__.insert()
        for data_set_class, progress in self._parse_dataset(dataset_path, 'name'):
            name_id = get_int(getattr(data_set_class, 'nconst'))
            self.name_ids.add(name_id)

            yield from self._get_name_title_data(data_set_class, name_id, progress)

            data_line = {
                "id": name_id,
                "primary_name": getattr(data_set_class, 'primaryName'),
                "birth_year": self._get_null(getattr(data_set_class, 'birthYear')),
                "death_year": self._get_null(getattr(data_set_class, 'deathYear')),
                "primary_profession": getattr(data_set_class, 'primaryProfession')
            }
            yield statement, data_line, progress

    def _get_name_title_data(self, data_set_class, name_id, progress):
        statement = models.NameTitle.insert()
        titles = [get_int(el) for el in getattr(data_set_class, 'knownForTitles').split(',') if get_int(el)]
        for title_id in titles:
            if title_id in self.title_ids:
                data_line = {
                    "name_id": name_id,
                    "title_id": title_id
                }
                yield statement, data_line, progress

    def _parse_principals(self, dataset_path):
        self.clean_table(models.Principals)
        statement = models.Principals.__table__.insert()
        for data_set_class, progress in self._parse_dataset(dataset_path, 'principals'):
            title_id = get_int(getattr(data_set_class, 'tconst'))
            name_id = get_int(getattr(data_set_class, 'nconst'))
            if title_id in self.title_ids and name_id in self.name_ids:
                data_line = {
                    "ordering": getattr(data_set_class, 'ordering'),
                    "category": getattr(data_set_class, 'category'),
                    "job": self._get_null(getattr(data_set_class, 'job')),
                    "characters": self._get_null(getattr(data_set_class, 'characters')),
                    "name_id": name_id,
                    "title_id": title_id,
                }
                yield statement, data_line, progress

    def _parse_ratings(self, dataset_path):
        self.clean_table(models.Ratings)
        statement = models.Ratings.__table__.insert()
        for data_set_class, progress in self._parse_dataset(dataset_path, 'ratings'):
            title_id = get_int(getattr(data_set_class, 'tconst'))
            if title_id in self.title_ids:
                data_line = {
                    "average_rating": getattr(data_set_class, 'averageRating'),
                    "num_votes": getattr(data_set_class, 'numVotes'),
                    "title_id": title_id,
                }
                yield statement, data_line, progress

    def _parse_dataset(self, file_path, table_name):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter='\t')
            data_set_class = namedtuple('_', next(tsv_reader))
            for line in tsv_reader:
                read_size += len(''.join(line)) + len(line)

                try:
                    data = data_set_class(*line)
                    yield data, (read_size / size) * 100
                except TypeError:
                    self.invalid_ids[table_name].add(get_int(line[0]))

    @staticmethod
    def _get_null(value):
        if value != '\\N':
            return value

    def clean_up(self):
        for table in reversed(self.metadata.sorted_tables):
            self.engine.execute(table.delete())

    def clean_table(self, model):
        print(f"Cleaning up table '{model.__tablename__}' ...")
        session = self._get_session()
        session.query(model).delete()
        session.commit()

# TODO: Implement data set fields validation
# TODO: Implement decorator with arguments instead of _get_parse_handler
