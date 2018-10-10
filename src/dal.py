import csv
from collections import namedtuple
from os.path import join, getsize, exists
import os
import sqlite3
import sys
import tempfile
from typing import Iterable, Iterator, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.models as models

SQLITE_TYPE = 'sqlite:///'
CURSOR_UP_ONE = '\x1b[1A'
ERASE_LINE = '\x1b[2K'


class ImdbDal:
    db_type = SQLITE_TYPE
    db_path = ':memory:'
    echo = False
    session = None
    dataset_paths: Dict = None

    def __init__(self, dataset_paths: Dict=None, root=None):
        self.engine = None
        self.metadata = None
        self.dataset_paths = self.dataset_paths or dataset_paths
        self.root = root or tempfile.gettempdir()

    def db_init(self, db_type=None, db_path=None):
        if exists(db_path):
            os.remove(db_path)
        self.db_path = db_path or self.db_path
        self.db_type = db_type or self.db_type

        if db_type == SQLITE_TYPE:
            sqlite3.connect(self.db_path)

        self.engine = create_engine(f'{self.db_type}{self.db_path}', echo=self.echo)
        self.metadata = models.Base.metadata
        self.metadata.create_all(bind=self.engine)
        self.metadata.reflect(bind=self.engine)
        session = sessionmaker(bind=self.engine)
        self.session = session()

    def parse_data_sets(self):
        if self.session is None:
            raise Exception('Database session is not initialized')
        for table_name, dataset_path in self.dataset_paths.items():
            status_line = f"Parsing {dataset_path} into '{table_name}' table ..."
            self._insert_dataset(getattr(self, f'_parse_{table_name}')(join(self.root, dataset_path)), status_line)
        self.session.commit()

    def _insert_dataset(self, dataset_iter: Iterator, status_line: str, bunch_size=100000):
        start_progress = 0
        print(f'{status_line}: 0%')
        for idx, (line, progress) in enumerate(dataset_iter):
            self.session.add(line)
            if progress - start_progress > 0.01:
                start_progress += 0.01
                sys.stdout.write(CURSOR_UP_ONE)
                sys.stdout.write(ERASE_LINE)
                print(f'{status_line}:{progress:.2f}%')

            if idx % bunch_size == 0:
                self.session.commit()

    def _parse_title(self, dataset_path):
        for data_set_class, progress in self._parse_dataset(dataset_path):
            title_line = models.Title(
                id=getattr(data_set_class, 'tconst'),
                titleType=getattr(data_set_class, 'titleType'),
                primaryTitle=getattr(data_set_class, 'primaryTitle'),
                originalTitle=getattr(data_set_class, 'originalTitle'),
                isAdult=bool(getattr(data_set_class, 'isAdult')),
                startYear=self._get_null(getattr(data_set_class, 'startYear')),
                endYear=self._get_null(getattr(data_set_class, 'endYear')),
                runtimeMinutes=getattr(data_set_class, 'runtimeMinutes'),
                genres=getattr(data_set_class, 'genres'),
            )
            yield title_line, progress

    def _parse_principals(self, dataset_path):
        for data_set_class, progress in self._parse_dataset(dataset_path):
            principals_line = models.Principals(
                ordering=getattr(data_set_class, 'ordering'),
                category=getattr(data_set_class, 'category'),
                job=self._get_null(getattr(data_set_class, 'job')),
                characters=self._get_null(getattr(data_set_class, 'characters')),
                title=self._get_title(getattr(data_set_class, 'tconst')),
                name=self._get_name(getattr(data_set_class, 'nconst'))
            )
            yield principals_line, progress

    def _parse_ratings(self, dataset_path):
        for data_set_class, progress in self._parse_dataset(dataset_path):
            ratings_line = models.Ratings(
                averageRating=getattr(data_set_class, 'averageRating'),
                numVotes=getattr(data_set_class, 'numVotes'),
                title=self._get_title(getattr(data_set_class, 'tconst'))
            )
            yield ratings_line, progress

    @staticmethod
    def _parse_dataset(file_path):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter='\t')
            data_set_class = namedtuple('_', next(tsv_reader))
            for line in tsv_reader:
                read_size += len(''.join(line)) + len(line)
                try:
                    yield data_set_class(*line), (read_size / size) * 100
                except TypeError:
                    print(f'Invalid line: {line}')
                    continue
                except Exception:
                    print(f'Invalid line: {line}')
                    raise

    def _parse_name(self, dataset_path):
        for data_set_class, progress in self._parse_dataset(dataset_path):
            name_line = models.Name(
                id=getattr(data_set_class, 'nconst'),
                primaryName=getattr(data_set_class, 'primaryName'),
                birthYear=getattr(data_set_class, 'birthYear'),
                deathYear=self._get_null(getattr(data_set_class, 'deathYear')),
                primaryProfession=getattr(data_set_class, 'primaryProfession'),
                titles=self._get_titles(getattr(data_set_class, 'knownForTitles').split(','))  # TODO: optimize
            )

            yield name_line, progress

    def _get_titles(self, title_ids: Iterable):
        query = self.session.query(models.Title)
        query = query.filter(models.Title.id.in_(title_ids))
        return query.all()

    def _get_title(self, title_id: str):
        query = self.session.query(models.Title).filter(models.Title.id == title_id)
        return query.one_or_none()

    def _get_name(self, name_id: str):
        query = self.session.query(models.Name).filter(models.Name.id == name_id)
        return query.one_or_none()

    @staticmethod
    def _get_null(value):
        if value != '\\N':
            return value

    def clean_up(self):
        for table in reversed(self.metadata.sorted_tables):
            self.engine.execute(table.delete())
