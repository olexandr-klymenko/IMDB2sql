from typing import Dict, List, Tuple

from sqlalchemy import create_engine

import src.models as models
from src.utils import get_csv_filename


class DatasetLoader:
    def __init__(self, cmd_args, config: Dict):
        self.root = cmd_args.root
        self.db_uri = cmd_args.dburi
        self.resume = cmd_args.resume
        self.debug = cmd_args.debug

        self.dataset_paths: List[Tuple] = config['dataset_paths'].items()
        if self.resume is not None:
            idx = [el[0] for el in self.dataset_paths].index(self.resume)
            self.dataset_paths = self.dataset_paths[idx:]
        if cmd_args.one:
            self.dataset_paths = [self.dataset_paths[0]]

        self.csv_extension = config['csv_extension']
        self.engine = None
        self.metadata = None

    def db_init(self):
        self.db_uri = self.db_uri
        self.engine = create_engine(f'{self.db_uri}')
        self.metadata = models.Base.metadata
        self.metadata.create_all(bind=self.engine)
        self.metadata.reflect(bind=self.engine)

    def _copy_table(self, table_name):
        print(f'Copying data to {table_name} table ...')
        with open(get_csv_filename(self.csv_extension, self.root, table_name), 'r') as csv_file:
            conn = create_engine(self.db_uri, echo=self.debug).raw_connection()
            cursor = conn.cursor()
            cmd = f'COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
            cursor.copy_expert(cmd, csv_file)
            conn.commit()

    def load_dataset(self):
        self.clean_up()
        for table_name, _ in self.dataset_paths:
            self._copy_table(table_name)
        self._copy_table(models.NameTitle.name)

    def clean_up(self):
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
