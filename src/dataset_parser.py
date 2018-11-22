import csv
from collections import defaultdict
from os.path import join, getsize
from pprint import pprint
from typing import Iterator, Dict

from src import models
from src.utils import overwrite_upper_line, get_int, get_null, get_csv_filename

TITLE_NAME = models.Title.__tablename__
NAME_NAME = models.Name.__tablename__
PRINCIPALS_NAME = models.Principals.__tablename__
RATINGS_NAME = models.Ratings.__tablename__
NAME_TITLE_NAME = models.NameTitle.name


class DatasetParser:
    def __init__(self, cmd_args, config: Dict):
        self.root = cmd_args.root
        self.errors = defaultdict(list)
        self.indices = defaultdict(set)
        self.debug = cmd_args.debug
        self.dataset_paths = config['dataset_paths'].items()
        self.csv_extension = config['csv_extension']

    def parse_dataset(self):
        for table_name, dataset_path in self.dataset_paths:
            parse_handler = self._get_parse_handler(table_name)
            dataset_iter = parse_handler(join(self.root, dataset_path))
            self._write_normalized_dataset(dataset_iter, dataset_path, table_name)

        if self.debug:
            print('Invalid dataset items:')
            pprint(dict(self.errors))

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    def _write_normalized_dataset(self, dataset_iter: Iterator, dataset_path: str, table_name: str):
        output_filename = get_csv_filename(self.csv_extension, self.root, table_name)
        with open(output_filename, 'w') as dataset_out:
            writer = csv.writer(dataset_out)
            status_line = f"Parsing '{dataset_path}' into '{output_filename}' ..."
            print(f'{self._get_progress_line(status_line, 0)} ...')
            for idx, (data_line, progress) in enumerate(dataset_iter):
                overwrite_upper_line(self._get_progress_line(status_line, progress))
                writer.writerow(data_line)
            overwrite_upper_line(
                f'{self._get_progress_line(status_line, 100)} done'
            )

    @staticmethod
    def _get_progress_line(status_line, progress):
        return f'{status_line}: {progress:.2f}%'

    def _parse_title(self, dataset_path):
        for data, progress in self._parse_raw_dataset(dataset_path):
            try:
                title_id = get_int(data['tconst'])
                data_line = (
                    title_id,
                    data['titleType'],
                    data['primaryTitle'],
                    data['originalTitle'],
                    bool(data['isAdult']),
                    get_null(data['startYear']),
                    get_null(data['endYear']),
                    get_null(data['runtimeMinutes']),
                    data['genres'],
                )
            except KeyError:
                self.errors[TITLE_NAME].append(data)
            else:
                self.indices[TITLE_NAME].add(title_id)
                yield data_line, progress

    def _parse_name(self, dataset_path):
        with open(join(self.root, f'{NAME_TITLE_NAME}.{self.csv_extension}'), 'w') as dataset_out:
            writer = csv.writer(dataset_out)
            for data, progress in self._parse_raw_dataset(dataset_path):
                try:
                    name_id = get_int(data['nconst'])
                    data_line = (
                        name_id,
                        data['primaryName'],
                        get_null(data['birthYear']),
                        get_null(data['deathYear']),
                        data['primaryProfession']
                    )
                except KeyError:
                    self.errors[NAME_NAME].append(data)
                else:
                    self.indices[NAME_NAME].add(name_id)
                    yield data_line, progress
                    writer.writerows(self._get_name_title_data(data, name_id))

    def _get_name_title_data(self, data, name_id):
        titles = [get_int(el) for el in data['knownForTitles'].split(',') if get_int(el)]
        for title_id in titles:
            if title_id in self.indices[TITLE_NAME]:
                data_line = (name_id, title_id)
                yield data_line

    def _parse_principals(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            title_id, name_id = get_int(data['tconst']), get_int(data['nconst'])
            if title_id in self.indices[TITLE_NAME] and name_id in self.indices[NAME_NAME]:
                data_line = (
                    idx,
                    data['ordering'],
                    data['category'],
                    get_null(data['job']),
                    get_null(data['characters']),
                    title_id,
                    name_id,
                )
                yield data_line, progress
            else:
                self.errors[PRINCIPALS_NAME].append(data)

    def _parse_ratings(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            title_id = get_int(data['tconst'])
            if title_id in self.indices[TITLE_NAME]:
                data_line = (
                    idx,
                    data['averageRating'],
                    data['numVotes'],
                    title_id
                )
                yield data_line, progress
            else:
                self.errors[RATINGS_NAME].append(data)

    @staticmethod
    def _parse_raw_dataset(file_path):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter='\t')
            headers = next(tsv_reader)
            for line in tsv_reader:
                read_size += len(''.join(line)) + len(line)
                data = dict(zip(headers, line))
                yield data, (read_size / size) * 100
