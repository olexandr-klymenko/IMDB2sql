import csv
import os
import sqlite3
from collections import namedtuple
from os.path import join, getsize, exists
import psutil
from typing import Iterable, Iterator, List, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.models as models
from src.utils import overwrite_upper_line, get_int

SQLITE_TYPE = 'sqlite:///'


class ImdbDal:
    def __init__(self, root, resume, free_mem: int, dataset_paths: List):
        self.engine = None
        self.metadata = None
        self.resume = resume
        if self.resume is not None:
            idx = [el[0] for el in dataset_paths].index(self.resume)
            self.dataset_paths = dataset_paths[idx:]
        else:
            self.dataset_paths = dataset_paths

        self.root = root
        self.free_mem = free_mem
        self.echo = False
        self.db_uri = None
        self.session = None
        self.name_title: List[Dict] = []

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
        session = sessionmaker(bind=self.engine)
        self.session = session()

    def parse_data_sets(self):
        if self.session is None:
            raise Exception('Database session is not initialized')
        for table_name, dataset_path in self.dataset_paths:
            status_line = f"Parsing {dataset_path} into '{table_name}' table ..."
            parse_handler = self._get_parse_handler(table_name)
            dataset = parse_handler(join(self.root, dataset_path))
            self._insert_dataset(dataset, status_line)

        self.session.commit()

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    def _insert_dataset(self, dataset_iter: Iterator, status_line: str):
        start_progress = 0
        print(f'{status_line}: 0%')
        for line, progress in dataset_iter:
            self.session.add(line)
            if progress - start_progress > 0.01:
                start_progress += 0.01
                overwrite_upper_line(f'{status_line}: {progress:.2f}%')

            if self._is_time_for_commit():
                overwrite_upper_line(f'{status_line}: {progress :.2f}% committing ...')

                if self.name_title:
                    self.engine.execute(models.NameTitle.insert(), self.name_title)
                    self.name_title = []

                self.session.commit()

                overwrite_upper_line(f'{status_line}: {progress :.2f}%')
        overwrite_upper_line(f'{status_line}: 100% Done')

    def _is_time_for_commit(self):
        vm = psutil.virtual_memory()
        return self.free_mem > vm.free

    def _parse_title(self, dataset_path):
        self.clean_table(models.Title)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            title_line = models.Title(
                id=get_int(getattr(data_set_class, 'tconst')),
                titleType=getattr(data_set_class, 'titleType'),
                primaryTitle=getattr(data_set_class, 'primaryTitle'),
                originalTitle=getattr(data_set_class, 'originalTitle'),
                isAdult=bool(getattr(data_set_class, 'isAdult')),
                startYear=self._get_null(getattr(data_set_class, 'startYear')),
                endYear=self._get_null(getattr(data_set_class, 'endYear')),
                runtimeMinutes=self._get_null(getattr(data_set_class, 'runtimeMinutes')),
                genres=getattr(data_set_class, 'genres'),
            )
            yield title_line, progress

    def _parse_principals(self, dataset_path):
        self.clean_table(models.Principals)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            principals_line = models.Principals(
                ordering=getattr(data_set_class, 'ordering'),
                category=getattr(data_set_class, 'category'),
                job=self._get_null(getattr(data_set_class, 'job')),
                characters=self._get_null(getattr(data_set_class, 'characters')),
                name_id=get_int(getattr(data_set_class, 'nconst')),
                title_id=get_int(getattr(data_set_class, 'tconst'))
            )
            yield principals_line, progress

    def _parse_ratings(self, dataset_path):
        self.clean_table(models.Ratings)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            ratings_line = models.Ratings(
                averageRating=getattr(data_set_class, 'averageRating'),
                numVotes=getattr(data_set_class, 'numVotes'),
                title_id=get_int(getattr(data_set_class, 'tconst'))
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
        self.clean_table(models.Name)
        for data_set_class, progress in self._parse_dataset(dataset_path):
            name_id = get_int(getattr(data_set_class, 'nconst'))
            titles = [get_int(el) for el in getattr(data_set_class, 'knownForTitles').split(',') if get_int(el)]
            for title_id in titles:
                self.name_title.append({'nameId': name_id, 'titleId': title_id})

            name_line = models.Name(
                id=get_int(getattr(data_set_class, 'nconst')),
                primaryName=getattr(data_set_class, 'primaryName'),
                birthYear=getattr(data_set_class, 'birthYear'),
                deathYear=self._get_null(getattr(data_set_class, 'deathYear')),
                primaryProfession=getattr(data_set_class, 'primaryProfession'),
            )

            yield name_line, progress

    @staticmethod
    def _get_null(value):
        if value != '\\N':
            return value

    def clean_up(self):
        for table in reversed(self.metadata.sorted_tables):
            self.engine.execute(table.delete())

    def clean_table(self, model):
        self.session.query(model).delete()
        self.session.commit()
