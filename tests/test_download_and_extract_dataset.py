import threading
import unittest

from src.utils import get_data_sets
from src.dataset_handler import DataSetsHandler
from tests.utils import (
    FakeHTTPServer,
    TEST_HTTP_PORT,
    TEST_TVS_DATA,
    TEST_FILENAME,
    TEST_FILENAME_INVALID,
)


class TestDownloadAndExtractDataset(unittest.TestCase):
    server: FakeHTTPServer
    downloader: DataSetsHandler

    @classmethod
    def setUpClass(cls):
        data_sets = get_data_sets(
            urls=[f"http://127.0.0.1:{TEST_HTTP_PORT}/{TEST_FILENAME}"],
        )
        cls.downloader = DataSetsHandler(data_sets)
        cls.server = FakeHTTPServer()
        thread = threading.Thread(target=cls.server.serve_forever)
        thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.socket.close()
        cls.server.shutdown()
        cls.downloader.cleanup()

    def test_download_and_extract_dataset(self):
        self.downloader.download()
        self.downloader.extract()
        for data_set in [el.extracted for el in self.downloader.data_sets]:
            with open(data_set) as f:
                self.assertEqual(TEST_TVS_DATA, f.read().strip("\n"))

    def test_invalid_data_set_filename(self):
        with self.assertRaises(ValueError):
            data_sets = get_data_sets(
                [f"http://127.0.0.1:{TEST_HTTP_PORT}/{TEST_FILENAME_INVALID}"]
            )
            DataSetsHandler(data_sets)
