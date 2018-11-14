import urllib.request
from argparse import ArgumentParser
from os import getcwd
from os.path import join

from src.constants import DATASET_PATHS, DEFAULT_MAX_MEMORY_FOOTPRINT
from src.dataset_parser import DatasetParser
from src.utils import get_config, get_links, DataSetsHandler


def main(cmd_args):
    if cmd_args.download or cmd_args.extract:
        config = get_config(join(getcwd(), 'config', 'datasets.yml'))

        with urllib.request.urlopen(config['data_sets_url']) as response:
            imdb_page_content = response.read()

        handler = DataSetsHandler(get_links(imdb_page_content, config), root=cmd_args.root)

        if cmd_args.download:
            handler.download()
        if cmd_args.extract:
            handler.extract()

    if cmd_args.parse:
        dal = DatasetParser(dataset_paths=DATASET_PATHS,
                            root=cmd_args.root,
                            max_footprint=cmd_args.maxfootprint,
                            resume=cmd_args.resume,
                            one=cmd_args.one)
        dal.db_init(db_uri=cmd_args.dburi)
        dal.parse_data_sets()


if __name__ == '__main__':
    cmd_line_parser = ArgumentParser()
    cmd_line_parser.add_argument('--root', '-r', help='Directory where data sets will be downloaded', required=True)
    cmd_line_parser.add_argument('--download', '-d', action="store_true")
    cmd_line_parser.add_argument('--extract', '-x', action="store_true")
    cmd_line_parser.add_argument('--parse', '-p', action="store_true")
    cmd_line_parser.add_argument('--maxfootprint', '-mf', default=DEFAULT_MAX_MEMORY_FOOTPRINT, type=int)
    cmd_line_parser.add_argument('--dburi', '-db', choices=[
        "postgresql://postgres@127.0.0.1:5432/postgres",
        "mysql+mysqlconnector://root:mysql@127.0.0.1:3306/mysql",
        "sqlite:///imdb.db"
    ], default='sqlite:///:memory:', help='Database URI')
    cmd_line_parser.add_argument('--resume', choices=['name', 'principals', 'ratings'], default=None,
                                 help='Start parsing not from first table')
    cmd_line_parser.add_argument('--one', help="Parse only one table", action="store_true")
    args = cmd_line_parser.parse_args()
    print(args)
    main(args)

# TODO:  implement click for better cli experience
