from functools import partial
from glob import glob
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Dict, List, Tuple

import src.models as models


def get_table_object(table):
    """
    Returns Table object
    :param table: Union[Table, Model]
    :return: Table object
    """
    try:
        _ = table.delete
    except AttributeError:
        return table.__table__
    else:
        return table


class DatasetLoader:
    def __init__(self, cmd_args, config: Dict):
        self.root = cmd_args.root
        self.db_uri = cmd_args.dburi
        self.resume = cmd_args.resume
        self.debug = cmd_args.debug
        self.quiet = cmd_args.quiet

        self.delimiter = config["dataset_delimiter"]

        self.dataset_paths: List[Tuple] = config["dataset_paths"].items()
        if self.resume is not None:
            idx = [el[0] for el in self.dataset_paths].index(self.resume)
            self.dataset_paths = list(self.dataset_paths)[idx:]

        self.csv_extension = config["csv_extension"]
        self.engine = None
        self.connection = None
        self.metadata = None

    def db_init(self):
        self.engine = models.db.create_engine(self.db_uri)
        self.connection = self.engine.raw_connection()
        self.metadata = models.db.metadata
        self.metadata.create_all(bind=self.engine)
        self.metadata.reflect(bind=self.engine)

    def load_dataset(self):
        self.clean_up()
        self._copy_table(models.JobModel.__tablename__)
        for table_name, _ in self.dataset_paths:
            self._copy_table(table_name)
        self._copy_table(models.PersonFilm.name)
        self._copy_table(models.ProfessionModel.__tablename__)
        self._copy_table(models.ProfessionPerson.name)
        self._copy_table(models.GenreModel.__tablename__)
        self._copy_table(models.GenreFilm.name)

    def clean_up(self):
        tables = self._get_sorted_tables(self.metadata.sorted_tables)
        for table in tables:
            table_obj = get_table_object(table)
            if not self.quiet:
                print(f"Cleaning up table '{table_obj.name}' ...")

            self.engine.execute(table_obj.delete())

            if table_obj.name == self.resume:
                break

    def _copy_table(self, table_name):
        if not self.quiet:
            print(f"Copying data to '{table_name}' table ...")
        handler = partial(self._copy_file, self.db_uri, table_name)
        with Pool(cpu_count()) as pool:
            pool.map(handler, glob(str(Path(self.root, table_name, "*"))))

    @staticmethod
    def _copy_file(db_uri: str, table_name: str, file_name: str):
        engine = models.db.create_engine(db_uri)
        connection = engine.raw_connection()
        with connection.cursor() as cursor:
            with open(file_name, "r") as csv_file:
                cursor.copy_from(csv_file, table_name, sep="\t")
        connection.commit()

    def _get_sorted_tables(self, tables):
        sorted_tables = []
        for data_set_name in reversed([el[0] for el in self.dataset_paths]):
            for table in tables:
                if table.name == data_set_name:
                    sorted_tables.append(table)
                    break

        sorted_tables.append(models.JobModel)
        sorted_tables.insert(0, models.PersonFilm)
        sorted_tables.insert(1, models.ProfessionPerson)
        sorted_tables.insert(2, models.GenreFilm)
        sorted_tables.insert(3, models.ProfessionModel)
        sorted_tables.insert(4, models.GenreModel)

        return sorted_tables
