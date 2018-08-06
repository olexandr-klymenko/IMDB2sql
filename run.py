import urllib.request
from os import getcwd
from os.path import join

from src.utils import get_config, get_links, DataSetsHandler

config = get_config(join(getcwd(), 'config', 'datasets.yml'))

with urllib.request.urlopen(config['data_sets_url']) as response:
    imdb_page_content = response.read()


handler = DataSetsHandler(get_links(imdb_page_content, config['data_sets_file_pattern']))

# handler.download()

handler.extract()
