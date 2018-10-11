import urllib.request
from os import getcwd
from os.path import join

from memory_profiler import profile

from src.dal import ImdbDal
from src.utils import get_config, get_links, DataSetsHandler

TITLES_DATASET = 'title.basics.tsv'
NAMES_DATASET = 'name.basics.tsv'
PRINCIPALS_DATASET = 'title.principals.tsv'
RATINGS_DATASET = 'title.ratings.tsv'


DATASET_PATHS = {'title': TITLES_DATASET,
                 'name': NAMES_DATASET,
                 'principals': PRINCIPALS_DATASET,
                 'ratings': RATINGS_DATASET}

ROOT = '/home/oklymenko/Downloads/IMDB'
# ROOT = '/home/oklymenko/Documents/IMDB2sql/tests/datasets'


@profile
def main():
    # config = get_config(join(getcwd(), 'config', 'datasets.yml'))
    #
    # with urllib.request.urlopen(config['data_sets_url']) as response:
    #     imdb_page_content = response.read()
    #
    # handler = DataSetsHandler(get_links(imdb_page_content, config), root=ROOT)
    #
    # handler.download()
    #
    # handler.extract()

    dal = ImdbDal(dataset_paths=DATASET_PATHS, root=ROOT, batch_size=1_000_000)
    dal.db_init(db_path=join(ROOT, 'imdb.db'))
    # dal.db_init()
    dal.parse_data_sets()


if __name__ == '__main__':
    main()
