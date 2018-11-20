import csv
import os
import sqlite3
from collections import defaultdict
from os.path import join, getsize, exists
from typing import Iterator, List

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import src.models as models
from src.utils import overwrite_upper_line, get_int

SQLITE_TYPE = 'sqlite:///'


class DatasetParser:
    def __init__(self, root, resume, dataset_paths: List, one: bool, dry_run: bool):
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
        self.dry_run = dry_run
        self.echo = False
        self.db_uri = None
        self.errors = defaultdict(set)

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

    def parse_data_sets(self):
        self.clean_up()
        for table_name, dataset_path in self.dataset_paths:
            parse_handler = self._get_parse_handler(table_name)
            dataset = parse_handler(join(self.root, dataset_path))
            self._insert_dataset(dataset, f"Parsing '{dataset_path}' into '{table_name}' table ...")

        print(f"Invalid dataset ids:\n\t {dict(self.errors)}")

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    def _insert_dataset(self, dataset_iter: Iterator, status_line: str):
        print(f'{self._get_status_line(status_line, 100)} ...')
        for idx, (statement, data_line, progress) in enumerate(dataset_iter):
            overwrite_upper_line(self._get_status_line(status_line, progress))
            if not self.dry_run:
                try:
                    self.engine.execute(statement, **data_line)
                except IntegrityError:
                    self.errors[statement.table.name] = data_line
        overwrite_upper_line(
            f'{self._get_status_line(status_line, 100)} done'
        )

    @staticmethod
    def _get_status_line(status_line, progress):
        return f'{status_line}: {progress:.2f}%'

    def _get_session(self):
        return sessionmaker(bind=self.engine)()

    def _parse_title(self, dataset_path):
        statement = models.Title.__table__.insert()
        for data, progress in self._parse_dataset(dataset_path):
            try:
                data_line = {
                    "id": get_int(data['tconst']),
                    "title_type": data['titleType'],
                    "primary_title": data['primaryTitle'],
                    "original_title": data['originalTitle'],
                    "is_adult": bool(data['isAdult']),
                    "start_year": self._get_null(data['startYear']),
                    "end_year": self._get_null(data['endYear']),
                    "runtime_minutes": self._get_null(data['runtimeMinutes']),
                    "genres": data['genres'],
                }
            except KeyError:
                pass
            else:
                yield statement, data_line, progress

    def _parse_name(self, dataset_path):
        statement = models.Name.__table__.insert()
        for data, progress in self._parse_dataset(dataset_path):
            name_id = get_int(data['nconst'])
            try:
                data_line = {
                    "id": name_id,
                    "primary_name": data['primaryName'],
                    "birth_year": self._get_null(data['birthYear']),
                    "death_year": self._get_null(data['deathYear']),
                    "primary_profession": data['primaryProfession']
                }
            except KeyError:
                pass
            else:
                yield statement, data_line, progress
                yield from self._get_name_title_data(data, name_id, progress)

    @staticmethod
    def _get_name_title_data(data, name_id, progress):
        statement = models.NameTitle.insert()
        titles = [get_int(el) for el in data['knownForTitles'].split(',') if get_int(el)]
        for title_id in titles:
            data_line = {
                "name_id": name_id,
                "title_id": title_id
            }
            yield statement, data_line, progress

    def _parse_principals(self, dataset_path):
        statement = models.Principals.__table__.insert()
        for data, progress in self._parse_dataset(dataset_path):
            data_line = {
                "ordering": data['ordering'],
                "category": data['category'],
                "job": self._get_null(data['job']),
                "characters": self._get_null(data['characters']),
                "name_id": get_int(data['nconst']),
                "title_id": get_int(data['tconst']),
            }
            yield statement, data_line, progress

    def _parse_ratings(self, dataset_path):
        statement = models.Ratings.__table__.insert()
        for data, progress in self._parse_dataset(dataset_path):
            data_line = {
                "average_rating": data['averageRating'],
                "num_votes": data['numVotes'],
                "title_id": get_int(data['tconst']),
            }
            yield statement, data_line, progress

    @staticmethod
    def _parse_dataset(file_path):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter='\t')
            headers = next(tsv_reader)
            for line in tsv_reader:
                read_size += len(''.join(line)) + len(line)
                data = dict(zip(headers, line))
                yield data, (read_size / size) * 100

    @staticmethod
    def _get_null(value):
        if value != '\\N':
            return value

    def clean_up(self):
        if not self.dry_run:
            tables = self._get_sorted_tables(self.metadata.sorted_tables)
            for table in tables:
                print(f"Cleaning up table '{table.name}' ...")
                self.engine.execute(table.delete())
                if table.name == self.resume:
                    break

    def _get_sorted_tables(self, tables):
        sorted_tables = []
        for data_set_name in reversed([el[0] for el in self.dataset_paths]):
            for table in tables:
                if table.name == data_set_name:
                    sorted_tables.append(table)
                    break
        sorted_tables.insert(0, models.NameTitle)
        return sorted_tables

# TODO: Implement data set fields validation
# TODO: Implement decorator with arguments instead of _get_parse_handler
