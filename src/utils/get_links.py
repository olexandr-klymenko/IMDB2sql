import gzip
import urllib.parse
import urllib.request
from os.path import join, dirname
from typing import List

from bs4 import BeautifulSoup


def get_links(dataset_index_page_content: str, datasets_file_pattern: str) -> List:
    bs_obj = BeautifulSoup(dataset_index_page_content, "html.parser")
    return [link.get('href') for link in bs_obj.find_all('a') if link.get('href').endswith(datasets_file_pattern)]


def download_and_extract_dataset(url: str) -> str:
    local_filename, headers = urllib.request.urlretrieve(url=url)
    with gzip.open(local_filename) as zf:
        dest_path = join(dirname(local_filename), urllib.parse.urlparse(url).path[1:])
        with open(dest_path, 'w') as f:
            f.write(zf.read().decode())
            return dest_path
