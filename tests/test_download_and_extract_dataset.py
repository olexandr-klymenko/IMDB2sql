import threading
import unittest
from os import remove

from src.utils import download_and_extract_dataset
from tests.utils import TestServer, TEST_HTTP_PORT, TEST_TVS_DATA, TEST_FILENAME, TEST_FILENAME_INVALID


class TestDownloadAndExtractDataset(unittest.TestCase):

    def setUp(self):
        self.server = TestServer()
        thread = threading.Thread(target=self.server.serve_forever)
        thread.start()
        self.dataset_path = None

    def tearDown(self):
        self.server.socket.close()
        self.server.shutdown()
        if self.dataset_path:
            remove(self.dataset_path)

    def test_download_and_extract_dataset(self):
        self.dataset_path = download_and_extract_dataset(f'http://127.0.0.1:{TEST_HTTP_PORT}/{TEST_FILENAME}')
        with open(self.dataset_path) as f:
            self.assertEqual(TEST_TVS_DATA, f.read().strip('\n'))

    def test_invalid_data_set_filename(self):
        with self.assertRaises(Exception):
            download_and_extract_dataset(f'http://127.0.0.1:{TEST_HTTP_PORT}/{TEST_FILENAME_INVALID}')
