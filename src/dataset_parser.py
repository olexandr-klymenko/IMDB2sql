from collections import defaultdict

import csv
import json
from os.path import join, getsize
from typing import Iterator, Dict

from src import models
from src.utils import overwrite_upper_line, get_int, get_null, get_csv_filename

TITLE = models.Title.__tablename__
NAME = models.Name.__tablename__
PRINCIPAL = models.Principal.__tablename__
RATING = models.Rating.__tablename__
NAME_TITLE = models.NameTitle.name
PROFESSION = models.Profession.__tablename__
NAME_PROFESSION = models.ProfessionName.name
GENRE = models.Genre.__tablename__
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

        self.profession_names = defaultdict(list)
        self.genre_titles = defaultdict(list)

    def parse_dataset(self):
        for table_name, dataset_path in self.dataset_paths:
            parse_handler = self._get_parse_handler(table_name)
            dataset_iter = parse_handler(join(self.root, dataset_path))
            self._write_normalized_dataset(dataset_iter, dataset_path, table_name)

        self._write_extra_data(PROFESSION, NAME_PROFESSION, self.profession_names)
        self._write_extra_data(GENRE, GENRE_TITLE, self.genre_titles)

        with open('errors.log', 'w') as ef:
            ef.write(json.dumps(self.errors))

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
                title_id = get_int(data['tconst'])
                data_line = (
                    title_id,
                    data['titleType'],
                    data['primaryTitle'],
                    data['originalTitle'],
                    bool(data['isAdult']),
                    get_null(data['startYear']),
                    get_null(data['endYear']),
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
        with open(join(self.root, f'{NAME_TITLE}.{self.csv_extension}'), 'w') as dataset_out:
            writer = csv.writer(dataset_out)
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
                    writer.writerows(self._get_name_title_data(data, name_id))

    def _update_professions(self, professions_from_dataset: str, name_id: int):
        for profession in professions_from_dataset.split(','):
            self.profession_names[profession].append(name_id)

    def _get_name_title_data(self, data, name_id):
        titles = [get_int(el) for el in data['knownForTitles'].split(',') if get_int(el)]
        for title_id in titles:
            if title_id in self.indices[TITLE]:
                data_line = (name_id, title_id)
                yield data_line

    def _parse_principal(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            title_id, name_id = get_int(data['tconst']), get_int(data['nconst'])
            if title_id in self.indices[TITLE] and name_id in self.indices[NAME]:
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
                self.errors[PRINCIPAL].append(data)

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
            else:
                self.errors[RATING].append(data)

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

    def _write_extra_data(self, table: str, mapper: str, extra_data: Dict):
        profession_filename = get_csv_filename(self.csv_extension, self.root, table)
        profession_name_filename = get_csv_filename(self.csv_extension, self.root, mapper)

        with open(profession_filename, 'w') as table_file:
            print(f"Writing {profession_filename} and {profession_name_filename} files ...")
            table_writer = csv.writer(table_file)
            with open(profession_name_filename, 'w') as mapper_file:
                mapper_writer = csv.writer(mapper_file)
                for idx, (field, table_ids) in enumerate(extra_data.items()):
                    table_writer.writerow([idx, field])
                    for table_id in table_ids:
                        mapper_writer.writerow([idx, table_id])

# TODO: Implement writing and reading to gzipped csv files
# TODO: Implement fast database cleanup
# TODO: Implement string fields size validation
