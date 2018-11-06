from argparse import ArgumentParser
import urllib.request
from os import getcwd
from os.path import join

from memory_profiler import profile

from src.constants import DATASET_PATHS
from src.dal import ImdbDal
from src.utils import get_config, get_links, DataSetsHandler

ROOT = '/home/oklymenko/Downloads/IMDB'


@profile
def main(args):
    if args.download or args.extract:
        config = get_config(join(getcwd(), 'config', 'datasets.yml'))

        with urllib.request.urlopen(config['data_sets_url']) as response:
            imdb_page_content = response.read()

        handler = DataSetsHandler(get_links(imdb_page_content, config), root=args.root)

        if args.download:
                handler.download()
        if args.extract:
            handler.extract()

    if args.parse:
        dal = ImdbDal(dataset_paths=DATASET_PATHS, root=args.root, batch_size=args.batch, resume=args.resume)
        dal.db_init(db_uri=args.dburi)
        dal.parse_data_sets()

# TODO: implement resumed table drop if exists


if __name__ == '__main__':
    cmd_line_parser = ArgumentParser()
    cmd_line_parser.add_argument('--root', help='Directory where data sets will be downloaded', default=ROOT)
    cmd_line_parser.add_argument('--download', action="store_true")
    cmd_line_parser.add_argument('--extract', action="store_true")
    cmd_line_parser.add_argument('--parse', action="store_true")
    cmd_line_parser.add_argument('--batch', default=1000_000)
    cmd_line_parser.add_argument('--dburi', default='sqlite:///:memory:',
                                 help="Database URI, i.e.: \n"
                                      "'postgresql://postgres@127.0.0.1:5432/postgres',\n"
                                      "'mysql+mysqlconnector://root:mysql@127.0.0.1:3306/mysql',\n"
                                      "'sqlite:///imdb.db'")
    cmd_line_parser.add_argument('--resume', choices=[None, 'name', 'principals', 'ratings'], default=None,
                                 help='Start parsing not from first table')
    args = cmd_line_parser.parse_args()
    main(args)
