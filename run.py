import urllib.request
from multiprocessing import Pool
from os import getcwd
from os.path import join

from src.utils import get_config, get_links, download_and_extract_dataset

config = get_config(join(getcwd(), 'config', 'datasets.yml'))

with urllib.request.urlopen(config['data_sets_url']) as response:
    imdb_page_content = response.read()

data_set_links = get_links(imdb_page_content, config['data_sets_file_pattern'])

with Pool(len(data_set_links)) as pool:
    print(pool.map(download_and_extract_dataset, data_set_links))
