from collections import defaultdict

import pprint
import csv
from os.path import join, getsize
from typing import Iterator, Dict, List, Set, Tuple

from src import models
from src.utils import overwrite_upper_line, get_int, get_null, get_csv_filename

TITLE = models.TitleModel.__tablename__
NAME = models.NameModel.__tablename__
PRINCIPAL = models.PrincipalModel.__tablename__
RATING = models.RatingModel.__tablename__
NAME_TITLE = models.NameTitle.name
PROFESSION = models.ProfessionModel.__tablename__
NAME_PROFESSION = models.ProfessionName.name
GENRE = models.GenreModel.__tablename__
GENRE_TITLE = models.GenreTitle.name


class DatasetParser:
    def __init__(self, cmd_args, config: Dict):
        self.root = cmd_args.root
        self.errors = defaultdict(list)
        self.indices = defaultdict(set)
        self.debug = cmd_args.debug
        self.quiet = cmd_args.quiet
        self.dataset_paths = config['dataset_paths'].items()
        self.delimiter = config['dataset_delimiter']
        self.csv_extension = config['csv_extension']
        self.title_filter = config['title_filter']

        self.profession_names = defaultdict(list)
        self.genre_titles = defaultdict(list)
        self.name_title: Set[Tuple] = set()

    def parse_dataset(self):
        for table_name, dataset_path in self.dataset_paths:
            parse_handler = self._get_parse_handler(table_name)
            dataset_iter = parse_handler(join(self.root, dataset_path))
            self._write_normalized_dataset(dataset_iter, dataset_path, table_name)

        self._write_extra_data(PROFESSION, NAME_PROFESSION, self.profession_names)
        self._write_extra_data(GENRE, GENRE_TITLE, self.genre_titles)
        self._write_name_title()

        with open('errors.log', 'w') as ef:
            pprint.pprint(dict(self.errors), ef)

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    def _write_normalized_dataset(self, dataset_iter: Iterator, dataset_path: str, table_name: str):
        output_filename = get_csv_filename(self.csv_extension, self.root, table_name)
        with open(output_filename, 'w') as dataset_out:
            writer = csv.writer(dataset_out)
            status_line = f"Parsing '{dataset_path}' into '{output_filename}' ..."
            if not self.quiet:
                print(f'{self._get_progress_line(status_line, 0)} ...')
            for idx, (data_line, progress) in enumerate(dataset_iter):
                overwrite_upper_line(self._get_progress_line(status_line, progress), self.quiet)
                writer.writerow(data_line)
            overwrite_upper_line(
                f'{self._get_progress_line(status_line, 100)} done', self.quiet
            )

    @staticmethod
    def _get_progress_line(status_line, progress):
        return f'{status_line}: {progress:.2f}%'

    def _parse_title(self, dataset_path):
        for data, progress in self._parse_raw_dataset(dataset_path):
            try:
                if data['titleType'] != self.title_filter:
                    continue

                title_id = get_int(data['tconst'])
                data_line = (
                    title_id,
                    data['primaryTitle'],
                    bool(data['isAdult']),
                    get_null(data['startYear']),
                    get_null(data['runtimeMinutes'])
                )
                genres_from_dataset = get_null(data['genres'])
            except KeyError:
                self.errors[TITLE].append(data)
            else:
                self.indices[TITLE].add(title_id)
                if genres_from_dataset is not None:
                    self._update_genres(genres_from_dataset, title_id)
                yield data_line, progress

    def _update_genres(self, genres_from_dataset: str, title_id: int):
        for genre in genres_from_dataset.split(','):
            self.genre_titles[genre].append(title_id)

    def _parse_name(self, dataset_path):
        for data, progress in self._parse_raw_dataset(dataset_path):
            try:
                name_id = get_int(data['nconst'])
                data_line = (
                    name_id,
                    data['primaryName'],
                    get_null(data['birthYear']),
                    get_null(data['deathYear']),
                )
                profession_from_dataset = get_null(data['primaryProfession'])
            except KeyError:
                self.errors[NAME].append(data)
            else:
                self.indices[NAME].add(name_id)
                if profession_from_dataset is not None:
                    self._update_professions(profession_from_dataset, name_id)
                yield data_line, progress

                for title_id in self._get_title_ids(data):
                    self.name_title.add((name_id, title_id))

    def _update_professions(self, professions_from_dataset: str, name_id: int):
        for profession in professions_from_dataset.split(','):
            self.profession_names[profession].append(name_id)

    def _get_title_ids(self, data):
        titles = [get_int(el) for el in data['knownForTitles'].split(',') if get_int(el)]
        for title_id in titles:
            if title_id in self.indices[TITLE]:
                yield title_id

    def _parse_principal(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            title_id, name_id = get_int(data['tconst']), get_int(data['nconst'])
            if title_id in self.indices[TITLE] and name_id in self.indices[NAME]:
                data_line = (
                    idx,
                    data['category'],
                    title_id,
                    name_id,
                )
                yield data_line, progress
                self.name_title.add((name_id, title_id))

    def _parse_rating(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            title_id = get_int(data['tconst'])
            if title_id in self.indices[TITLE]:
                data_line = (
                    idx,
                    data['averageRating'],
                    data['numVotes'],
                    title_id
                )
                yield data_line, progress

    def _parse_raw_dataset(self, file_path):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter=self.delimiter)
            headers = next(tsv_reader)
            for line in tsv_reader:
                read_size += len(''.join(line)) + len(line)
                data = dict(zip(headers, line))
                yield data, (read_size / size) * 100

    def _write_name_title(self):
        with open(join(self.root, f'{NAME_TITLE}.{self.csv_extension}'), 'w') as dataset_out:
            writer = csv.writer(dataset_out)
            writer.writerows(self.name_title)

    def _write_extra_data(self, table: str, mapper: str, extra_data: Dict):
        table_filename = get_csv_filename(self.csv_extension, self.root, table)
        mapper_filename = get_csv_filename(self.csv_extension, self.root, mapper)

        with open(table_filename, 'w') as table_file:
            print(f"Dumping to {table_filename} and {mapper_filename} files ...")
            table_writer = csv.writer(table_file)
            with open(mapper_filename, 'w') as mapper_file:
                mapper_writer = csv.writer(mapper_file)
                for idx, (field, table_ids) in enumerate(extra_data.items()):
                    table_writer.writerow([idx, field])
                    for table_id in table_ids:
                        mapper_writer.writerow([idx, table_id])

# TODO: Implement writing and reading to gzipped csv files
# TODO: Implement string fields size validation
