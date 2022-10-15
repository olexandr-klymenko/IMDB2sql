import csv
import pprint
import subprocess
from collections import defaultdict
from functools import partial
from glob import glob
from multiprocessing import Pool, cpu_count
from os import remove
from os.path import join, getsize
from pathlib import Path
from typing import Iterator, Dict, Set, Tuple, Iterable, List

from src import models
from src.utils import overwrite_upper_line, get_int, get_null

FILM = models.FilmModel.__tablename__
PERSON = models.PersonModel.__tablename__
PRINCIPAL = models.PrincipalModel.__tablename__
RATING = models.RatingModel.__tablename__
PERSON_FILM = models.PersonFilm.name
PROFESSION = models.ProfessionModel.__tablename__
PERSON_PROFESSION = models.ProfessionPerson.name
GENRE = models.GenreModel.__tablename__
GENRE_FILM = models.GenreFilm.name
JOB = models.JobModel.__tablename__


def get_csv_filename(csv_extension, root, table_name):
    return join(root, f"{table_name}.{csv_extension}")


class DatasetParser:

    def __init__(self, cmd_args, config: Dict):
        self.root = cmd_args.root
        self.errors = defaultdict(list)
        self.indices = defaultdict(set)
        self.debug = cmd_args.debug
        self.quiet = cmd_args.quiet
        self.dataset_paths = config["dataset_paths"].items()
        self.delimiter = config["dataset_delimiter"]
        self.csv_extension = config["csv_extension"]
        self.film_filter: List = config["film_filter"]

        self.profession_person = defaultdict(list)
        self.genre_film = defaultdict(list)
        self.person_film: Set[Tuple] = set()
        self.jobs: Dict = {}

    def parse_dataset(self):
        for table_name, dataset_path in self.dataset_paths:
            parse_handler = self._get_parse_handler(table_name)
            dataset_iter = parse_handler(join(self.root, dataset_path))
            self._write_normalized_dataset(dataset_iter, dataset_path, table_name)

        self._write_extra_data(PROFESSION, PERSON_PROFESSION, self.profession_person)
        self._write_extra_data(GENRE, GENRE_FILM, self.genre_film)
        self._write_data(PERSON_FILM, self.person_film)
        self._write_data(JOB, [(value, key) for key, value in self.jobs.items()])

        self._split_all()

        with open("errors.log", "w") as ef:
            pprint.pprint(dict(self.errors), ef)

    def _get_parse_handler(self, table_name):
        return getattr(self, f"_parse_{table_name}")

    def _write_normalized_dataset(
            self, dataset_iter: Iterator, dataset_path: str, table_name: str
    ):
        output_filename = get_csv_filename(self.csv_extension, self.root, table_name)
        with open(output_filename, "w") as dataset_out:
            writer = self._get_csv_writer(dataset_out)
            status_line = f"Parsing '{dataset_path}' into '{output_filename}' ..."
            if not self.quiet:
                print(f"{self._get_progress_line(status_line, 0)} ...")
            for idx, (data_line, progress) in enumerate(dataset_iter):
                overwrite_upper_line(
                    self._get_progress_line(status_line, progress), self.quiet
                )
                writer.writerow(data_line)
            overwrite_upper_line(
                f"{self._get_progress_line(status_line, 100)} done", self.quiet
            )

    @staticmethod
    def _get_progress_line(status_line, progress):
        return f"{status_line}: {progress:.2f}%"

    def _parse_film(self, dataset_path):
        for data, progress in self._parse_raw_dataset(dataset_path):
            try:
                if data["titleType"] not in self.film_filter:
                    continue

                film_id = get_int(data["tconst"])
                data_line = (
                    film_id,
                    data["primaryTitle"],
                    bool(data["isAdult"]),
                    get_null(data["startYear"]),
                    get_null(data["runtimeMinutes"]),
                )
                genres_from_dataset = get_null(data["genres"])
            except KeyError:
                self.errors[FILM].append(data)
            else:
                self.indices[FILM].add(film_id)
                if genres_from_dataset is not None:
                    self._update_genres(genres_from_dataset, film_id)
                yield data_line, progress

    def _update_genres(self, genres_from_dataset: str, film_id: int):
        for genre in genres_from_dataset.split(","):
            self.genre_film[genre].append(film_id)

    def _parse_person(self, dataset_path):
        for data, progress in self._parse_raw_dataset(dataset_path):
            try:
                person_id = get_int(data["nconst"])
                data_line = (
                    person_id,
                    data["primaryName"],
                    get_null(data["birthYear"]),
                    get_null(data["deathYear"]),
                )
                profession_from_dataset = get_null(data["primaryProfession"])
            except KeyError:
                self.errors[PERSON].append(data)
            else:
                self.indices[PERSON].add(person_id)
                if profession_from_dataset is not None:
                    self._update_professions(profession_from_dataset, person_id)
                yield data_line, progress

                for film_id in self._get_film_ids(data):
                    self.person_film.add((person_id, film_id))

    def _update_professions(self, professions_from_dataset: str, person_id: int):
        for profession in professions_from_dataset.split(","):
            self.profession_person[profession].append(person_id)

    def _get_film_ids(self, data):
        titles = [
            get_int(el) for el in data["knownForTitles"].split(",") if get_int(el)
        ]
        for film_id in titles:
            if film_id in self.indices[FILM]:
                yield film_id

    def _parse_principal(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            film_id, person_id = get_int(data["tconst"]), get_int(data["nconst"])
            if film_id in self.indices[FILM] and person_id in self.indices[PERSON]:
                job = data["category"]
                self._update_jobs(job)
                data_line = (idx, film_id, person_id, self.jobs[job])
                yield data_line, progress
                self.person_film.add((person_id, film_id))

    def _update_jobs(self, job):
        if job not in self.jobs:
            self.jobs[job] = len(self.jobs) + 1

    def _parse_rating(self, dataset_path):
        for idx, (data, progress) in enumerate(self._parse_raw_dataset(dataset_path)):
            film_id = get_int(data["tconst"])
            if film_id in self.indices[FILM]:
                data_line = (idx, data["averageRating"], data["numVotes"], film_id)
                yield data_line, progress

    def _parse_raw_dataset(self, file_path):
        size = getsize(file_path)
        read_size = 0
        with open(file_path) as fd:
            tsv_reader = csv.reader(fd, delimiter=self.delimiter)
            headers = next(tsv_reader)
            for line in tsv_reader:
                read_size += len("".join(line)) + len(line)
                data = dict(zip(headers, line))
                yield data, (read_size / size) * 100

    def _write_data(self, table_name: str, data: Iterable[Tuple]):
        file_name = join(self.root, f"{table_name}.{self.csv_extension}")
        with open(file_name, "w") as dataset_out:
            print(f"Dumping to f'{file_name}' file ...")
            writer = self._get_csv_writer(dataset_out)
            writer.writerows(data)

    def _write_extra_data(self, table: str, mapper: str, extra_data: Dict):
        table_filename = get_csv_filename(self.csv_extension, self.root, table)
        mapper_filename = get_csv_filename(self.csv_extension, self.root, mapper)

        with open(table_filename, "w") as table_file:
            print(f"Dumping to {table_filename} and {mapper_filename} files ...")
            table_writer = self._get_csv_writer(table_file)
            with open(mapper_filename, "w") as mapper_file:
                mapper_writer = self._get_csv_writer(mapper_file)
                for idx, (field, table_ids) in enumerate(extra_data.items()):
                    table_writer.writerow([idx, field])
                    mapper_writer.writerows((idx, table_id) for table_id in table_ids)

    def _get_csv_writer(self, file_obj):
        return csv.writer(file_obj, delimiter=self.delimiter)

    def _split_all(self):
        processes = cpu_count()
        split_worker = partial(self._split_file, processes)
        with Pool(processes) as pool:
            pool.map(
                split_worker, glob(str(Path(self.root, f"*.{self.csv_extension}")))
            )
    @staticmethod
    def _split_file(processes: int, path: str):
        _path = Path(path)
        chunks_dir = _path.parent / _path.stem
        subprocess.call(["mkdir", "-p", str(chunks_dir)])
        lines_count = (
                              int(subprocess.check_output(["wc", "-l", path]).split()[0]) // processes
                      ) + 1
        subprocess.call(
            [
                "split",
                path,
                "-d",
                "-l",
                str(lines_count),
                f"{str(chunks_dir / _path.name)}.",
            ]
        )
        remove(path)


