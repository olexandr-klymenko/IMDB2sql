import csv
from collections import defaultdict
from os.path import join, getsize
from typing import Iterator, List

from src.constants import CSV_EXTENSION
from src.utils import overwrite_upper_line, get_int, get_csv_filename


class DatasetParser:
    def __init__(self, root, dataset_paths: List):
        self.dataset_paths = dataset_paths
        self.root = root
        self.errors = defaultdict(set)
        self.indices = defaultdict(set)

    def parse_dataset(self):
        for table_name, dataset_path in self.dataset_paths:
            parse_handler = self._get_parse_handler(table_name)
            dataset_iter = parse_handler(join(self.root, dataset_path))
            self._insert_dataset(dataset_iter, dataset_path, table_name)

    def _get_parse_handler(self, table_name):
        return getattr(self, f'_parse_{table_name}')

    def _insert_dataset(self, dataset_iter: Iterator, dataset_path: str, table_name: str):
        output_filename = get_csv_filename(self.root, table_name)
        status_line = f"Parsing '{dataset_path}' into '{output_filename}' ..."
        with open(output_filename, 'w') as dataset_out:
            writer = csv.writer(dataset_out)
            print(f'{self._get_status_line(status_line, 0)} ...')
            for idx, (data_line, progress) in enumerate(dataset_iter):
                overwrite_upper_line(self._get_status_line(status_line, progress))
                writer.writerow(data_line)
            overwrite_upper_line(
                f'{self._get_status_line(status_line, 100)} done'
            )

    @staticmethod
    def _get_status_line(status_line, progress):
        return f'{status_line}: {progress:.2f}%'

    def _parse_title(self, dataset_path):
        for data, progress in self._parse_dataset(dataset_path):
            try:
                title_id = get_int(data['tconst'])
                data_line = (
                    title_id,
                    data['titleType'],
                    data['primaryTitle'],
                    data['originalTitle'],
                    bool(data['isAdult']),
                    self._get_null(data['startYear']),
                    self._get_null(data['endYear']),
                    self._get_null(data['runtimeMinutes']),
                    data['genres'],
                )
            except KeyError:
                pass
            else:
                self.indices['title'].add(title_id)
                yield data_line, progress

    def _parse_name(self, dataset_path):
        with open(join(self.root, f'name_title.{CSV_EXTENSION}'), 'w') as dataset_out:
            writer = csv.writer(dataset_out)
            for data, progress in self._parse_dataset(dataset_path):
                try:
                    name_id = get_int(data['nconst'])
                    data_line = (
                        name_id,
                        data['primaryName'],
                        self._get_null(data['birthYear']),
                        self._get_null(data['deathYear']),
                        data['primaryProfession']
                    )
                except KeyError:
                    pass
                else:
                    self.indices['name'].add(name_id)
                    yield data_line, progress
                    writer.writerows(self._get_name_title_data(data, name_id))

    def _get_name_title_data(self, data, name_id):
        titles = [get_int(el) for el in data['knownForTitles'].split(',') if get_int(el)]
        for title_id in titles:
            if title_id in self.indices['title']:
                data_line = (name_id, title_id)
                yield data_line

    def _parse_principals(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_dataset(dataset_path)):
            name_id, title_id = get_int(data['nconst']), get_int(data['tconst'])
            if name_id in self.indices['name'] and title_id in self.indices['title']:
                data_line = (
                    idx,
                    data['ordering'],
                    data['category'],
                    self._get_null(data['job']),
                    self._get_null(data['characters']),
                    title_id,
                    name_id,
                )
                yield data_line, progress

    def _parse_ratings(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_dataset(dataset_path)):
            title_id = get_int(data['tconst'])
            if title_id in self.indices['title']:
                data_line = (
                    idx,
                    data['averageRating'],
                    data['numVotes'],
                    title_id
                )
                yield data_line, progress

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


# TODO: Implement data set fields validation
# TODO: Implement decorator with arguments instead of _get_parse_handler
