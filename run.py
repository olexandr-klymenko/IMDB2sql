from argparse import ArgumentParser
import urllib.request
from os import getcwd
from os.path import join

from memory_profiler import profile

from src.constants import DATASET_PATHS, DEFAULT_BATCH_SIZE
from src.dal import ImdbDal
from src.utils import get_config, get_links, DataSetsHandler


@profile
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
        dal = ImdbDal(dataset_paths=DATASET_PATHS,
                      root=cmd_args.root,
                      batch_size=cmd_args.batch,
                      resume=cmd_args.resume)
        dal.db_init(db_uri=cmd_args.dburi)
        dal.parse_data_sets()


if __name__ == '__main__':
    cmd_line_parser = ArgumentParser()
    cmd_line_parser.add_argument('--root', help='Directory where data sets will be downloaded', required=True)
    cmd_line_parser.add_argument('--download', action="store_true")
    cmd_line_parser.add_argument('--extract', action="store_true")
    cmd_line_parser.add_argument('--parse', action="store_true")
    cmd_line_parser.add_argument('--batch', default=DEFAULT_BATCH_SIZE, type=int)
    cmd_line_parser.add_argument('--dburi', default='sqlite:///:memory:',
                                 help="Database URI, i.e.: \n"
                                      "'postgresql://postgres@127.0.0.1:5432/postgres',\n"
                                      "'mysql+mysqlconnector://root:mysql@127.0.0.1:3306/mysql',\n"
                                      "'sqlite:///imdb.db'")
    cmd_line_parser.add_argument('--resume', choices=['name', 'principals', 'ratings'], default=None,
                                 help='Start parsing not from first table')
    args = cmd_line_parser.parse_args()
    main(args)
