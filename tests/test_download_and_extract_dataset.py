import threading
import unittest

from src.utils import DataSetsHandler
from tests.utils import (
    FakeHTTPServer,
    TEST_HTTP_PORT,
    TEST_TVS_DATA,
    TEST_FILENAME,
    TEST_FILENAME_INVALID,
)


class TestDownloadAndExtractDataset(unittest.TestCase):
    server: FakeHTTPServer = None
    downloader: DataSetsHandler = None

    @classmethod
    def setUpClass(cls):
        cls.downloader = DataSetsHandler(
            [f"http://127.0.0.1:{TEST_HTTP_PORT}/{TEST_FILENAME}"]
        )
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
        with self.assertRaises(Exception):
            DataSetsHandler(
                [f"http://127.0.0.1:{TEST_HTTP_PORT}/{TEST_FILENAME_INVALID}"]
            )
