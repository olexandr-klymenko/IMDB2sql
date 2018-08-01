import gzip
import re
import urllib.parse
import urllib.request
from os.path import join, dirname, exists
import os
import yaml
import tempfile
from typing import List

from bs4 import BeautifulSoup

DATA_SET_FILENAME_PATTERN = re.compile('^/(.*).gz')


def get_config(config_path):
    with open(config_path) as cfg:
        return yaml.load(cfg)


def get_links(dataset_index_page_content: str, datasets_file_pattern: str) -> List:
    bs_obj = BeautifulSoup(dataset_index_page_content, "html.parser")
    return [link.get('href') for link in bs_obj.find_all('a') if link.get('href').endswith(datasets_file_pattern)]


def download_and_extract_dataset(url: str, local_filename: str=None) -> str:
    raw_filepath = urllib.parse.urlparse(url).path
    file_path_re = DATA_SET_FILENAME_PATTERN.search(raw_filepath)
    if file_path_re is None:
        raise Exception("Data set filename doesn't match")

    try:
        if local_filename is None:
            _, local_filename = tempfile.mkstemp()

        urllib.request.urlretrieve(url=url, filename=local_filename)
        with gzip.open(local_filename) as zf:
            filename = file_path_re.group(1)
            dest_path = join(dirname(local_filename), filename)
            with open(dest_path, 'w') as f:
                f.write(zf.read().decode())
                os.remove(local_filename)
                return dest_path
    finally:
        if exists(local_filename):
            os.remove(local_filename)
