from typing import List

from bs4 import BeautifulSoup


def get_links(dataset_index_page_content: str, datasets_file_pattern: str) -> List:
    bs_obj = BeautifulSoup(dataset_index_page_content, "html.parser")
    return [link.get('href') for link in bs_obj.find_all('a') if link.get('href').endswith(datasets_file_pattern)]
