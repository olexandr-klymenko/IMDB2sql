import gzip
import os
import re
import sys
import tempfile
import urllib.parse
import urllib.request
from collections import namedtuple
from multiprocessing import Pool, cpu_count
from os.path import join, exists
from typing import List, Dict, Union

import yaml

DATA_SET_FILENAME_PATTERN = re.compile("^/(.*).gz")
CURSOR_UP_ONE = "\x1b[1A"
ERASE_LINE = "\x1b[2K"


def get_config(config_path):
    with open(config_path) as cfg:
        return yaml.load(cfg)


def get_links(dataset_index_page_content: str, config: Dict) -> List:
    from bs4 import BeautifulSoup

    bs_obj = BeautifulSoup(dataset_index_page_content, "html.parser")
    return [
        link.get("href") for link in bs_obj.find_all("a") if _filter_links(link, config)
    ]


def _filter_links(link, config) -> bool:
    dataset_files = [
        f"{el}.{config['dataset_file_ext']}" for el in config["dataset_paths"].values()
    ]
    return urllib.parse.urlparse(link.get("href")).path.strip("/") in dataset_files


DataSet = namedtuple("DataSet", ["url", "gzipped", "extracted"])


class DataSetsHandler:
    def __init__(self, urls, root=None):
        self.root = root or tempfile.gettempdir()
        self.data_sets: List[DataSet] = []
        self._init_data_sets(urls)

    def _init_data_sets(self, urls):
        for url in urls:
            path = urllib.parse.urlparse(url).path
            file_path_re = DATA_SET_FILENAME_PATTERN.search(path)
            if file_path_re is None:
                raise Exception("Data set filename doesn't match")
            gzipped = join(self.root, path.lstrip("/"))
            extracted = join(self.root, file_path_re.group(1))
            self.data_sets.append(
                DataSet(gzipped=gzipped, extracted=extracted, url=url)
            )

    def download(self):
        print("Downloading ...")
        Pool().map(self._download_file, self.data_sets)

    @staticmethod
    def _download_file(data_set: DataSet):
        print(f"{data_set.url} -> {data_set.gzipped} ...")
        urllib.request.urlretrieve(url=data_set.url, filename=data_set.gzipped)

    def extract(self):
        print("Extracting ...")
        with Pool(cpu_count()) as pool:
            pool.map(self._extract_file, self.data_sets)

    @staticmethod
    def _extract_file(data_set: DataSet):
        print(f"{data_set.gzipped} -> {data_set.extracted} ...")
        with gzip.open(data_set.gzipped) as zf:
            with open(data_set.extracted, "w") as f:
                for line in zf:
                    f.write(line.decode())
                os.remove(data_set.gzipped)

    def cleanup(self):
        for data_set in self.data_sets:
            if exists(data_set.gzipped):
                os.remove(data_set.gzipped)

            if exists(data_set.extracted):
                os.remove(data_set.extracted)


def overwrite_upper_line(content: str, quiet=False):
    """
    Output string content in the current line by overwriting
    :param content: string content
    :param quiet: bool
    :return:
    """
    if not quiet:
        sys.stdout.write(CURSOR_UP_ONE)
        sys.stdout.write(ERASE_LINE)
        print(content)


def get_int(id_: str) -> Union[int, None]:
    """
    Convert string id like tt0000002 to integer 2
    :param id_: string id
    :return: integer id
    """
    try:
        return int(id_[2:])
    except ValueError:
        return None


def get_null(value: str):
    if value.strip() not in ["\\N", ""]:
        return value
    return "0"


def get_csv_filename(csv_extension, root, table_name):
    return join(root, f"{table_name}.{csv_extension}")


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
