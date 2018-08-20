import csv
from collections import namedtuple
from typing import Iterable, Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import src.models as models


class ImdbDal:
    tables = [models.Title, models.Name, models.Principals]
    conn_string = 'sqlite:///:memory:'
    echo = True
    session = None

    def __init__(self, titles_path, names_path, principals_path):
        self.titles_path = titles_path
        self.names_path = names_path
        self.principals_path = principals_path

    def db_init(self, conn_string=None):
        engine = create_engine(conn_string or self.conn_string, echo=self.echo)
        models.Base.metadata.create_all(bind=engine)
        session = sessionmaker(bind=engine)
        self.session = session()

    def parse_data_sets(self):
        self._insert_dataset(self._parse_titles())
        self._insert_dataset(self._parse_names())
        self._insert_dataset(self._parse_principals())
        self.session.commit()

    def _insert_dataset(self, dataset_iter: Iterator):
        for line in dataset_iter:
            self.session.add(line)

    def _parse_titles(self):
        for data_set_class in self._parse_dataset(self.titles_path):
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
            yield title_line

    def _parse_principals(self):
        for data_set_class in self._parse_dataset(self.principals_path):
            principals_line = models.Principals(
                ordering=getattr(data_set_class, 'ordering'),
                category=getattr(data_set_class, 'category'),
                job=self._get_null(getattr(data_set_class, 'job')),
                characters=self._get_null(getattr(data_set_class, 'characters')),
                title=self._get_title(getattr(data_set_class, 'tconst')),
                name=self._get_name(getattr(data_set_class, 'nconst'))
            )
            yield principals_line

    @staticmethod
    def _parse_dataset(file_path):
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter='\t')
            data_set_class = namedtuple('_', next(tsv_reader))
            for line in tsv_reader:
                try:
                    yield data_set_class(*line)
                except TypeError:
                    print(f'Invalid line: {line}')
                    continue
                except Exception as err:
                    print(f'Invalid line: {line}')
                    raise

    def _parse_names(self):
        for data_set_class in self._parse_dataset(self.names_path):
            name_line = models.Name(
                id=getattr(data_set_class, 'nconst'),
                primaryName=getattr(data_set_class, 'primaryName'),
                birthYear=getattr(data_set_class, 'birthYear'),
                deathYear=self._get_null(getattr(data_set_class, 'deathYear')),
                primaryProfession=getattr(data_set_class, 'primaryProfession'),
                titles=self._get_titles(getattr(data_set_class, 'knownForTitles').split(','))
            )

            yield name_line

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
        [self.session.query(table).delete() for table in self.tables]
        self.session.commit()
