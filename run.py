from argparse import ArgumentParser
from os import getcwd
from os.path import join
from pathlib import Path

from src.utils import get_config, get_data_sets

CONFIG = get_config(join(getcwd(), "config", "config.yml"))


def main(cmd_args):
    if cmd_args.download or cmd_args.extract:
        import urllib.request
        from src.utils import get_config, get_links
        from src.dataset_handler import DataSetsHandler

        with urllib.request.urlopen(CONFIG["data_sets_url"]) as response:
            imdb_page_content = response.read()

        data_sets = get_data_sets(
            urls=get_links(imdb_page_content, CONFIG), root=Path(cmd_args.root)
        )

        handler = DataSetsHandler(data_sets)

        if cmd_args.download:
            handler.download()

        if cmd_args.extract:
            handler.extract()

    if cmd_args.parse:
        from src.dataset_parser import DatasetParser

        parser = DatasetParser(cmd_args, config=CONFIG)
        parser.parse_dataset()

    if cmd_args.load:
        from src.dataset_loader import DatasetLoader

        loader = DatasetLoader(cmd_args, config=CONFIG)
        loader.db_init()
        loader.load_dataset()


if __name__ == "__main__":
    cmd_line_parser = ArgumentParser()
    cmd_line_parser.add_argument(
        "--root",
        "-r",
        help="Directory where data sets will be downloaded",
        required=True,
    )
    cmd_line_parser.add_argument("--download", "-d", action="store_true")
    cmd_line_parser.add_argument("--extract", "-x", action="store_true")
    cmd_line_parser.add_argument("--parse", "-p", action="store_true")
    cmd_line_parser.add_argument("--load", "-l", action="store_true")
    cmd_line_parser.add_argument(
        "--dburi", "-db", default=CONFIG["default_database_uri"], help="Database URI"
    )
    cmd_line_parser.add_argument(
        "--resume",
        choices=["name", "principal", "rating"],
        default=None,
        help="Start parsing not from first table",
    )
    cmd_line_parser.add_argument("--debug", "-dd", action="store_true")
    cmd_line_parser.add_argument("--quiet", "-q", action="store_true")
    args = cmd_line_parser.parse_args()
    print(args)
    main(args)

# TODO: implement click for better cli experience
# TODO: implement alembic, invoke
# TODO: investigate polling db operation to get progress
# TODO: implement pytest instead of UnitTest
# TODO: implement rich (colored text)
