import unittest
from os.path import join

import validators

from src.utils import get_links, get_config
from tests.utils import get_root_dir, CONFIG_REL_PATH

CONFIG = get_config(join(get_root_dir(), CONFIG_REL_PATH))


class TestGetLinks(unittest.TestCase):
    def setUp(self):
        self.dataset_index_page_content = """
<html>
  <head>
    <meta name="robots" content="noindex">
  </head>
  <body>
    <h3>IMDb data files available for download</h3>
Documentation for these data files can be found on <a href=http://www.imdb.com/interfaces/> http://www.imdb.com/interfaces/ </a>
    <ul><a href=https://datasets.imdbws.com/name.basics.tsv.gz>name.basics.tsv.gz</a></ul>
    <ul><a href=https://datasets.imdbws.com/title.akas.tsv.gz>title.akas.tsv.gz</a></ul>
    <ul><a href=https://datasets.imdbws.com/title.basics.tsv.gz>title.basics.tsv.gz</a></ul>
    <ul><a href=https://datasets.imdbws.com/title.crew.tsv.gz>title.crew.tsv.gz</a></ul>
    <ul><a href=https://datasets.imdbws.com/title.episode.tsv.gz>title.episode.tsv.gz</a></ul>
    <ul><a href=https://datasets.imdbws.com/title.principals.tsv.gz>title.principals.tsv.gz</a></ul>
    <ul><a href=https://datasets.imdbws.com/title.ratings.tsv.gz>title.ratings.tsv.gz</a></ul>
  </body>
</html>
        """
        self.config = CONFIG

    def test_get_links(self):
        links = get_links(self.dataset_index_page_content, self.config)
        self.assertIsNotNone(links)
        self.assertIsInstance(links, list)
        self.assertGreater(len(links), 0)

        are_valid_urls = [validators.url(el) for el in links]

        self.assertTrue(len(set(are_valid_urls)) == 1)
        self.assertTrue(are_valid_urls[0])

        self.assertTrue(len(links), len(self.config["dataset_paths"]))
