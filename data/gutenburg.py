"""Scrape top 100 authors"""
import logging
import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
from typing import List, Generator, Dict, Union


class ScraperGutenburg:
    """
    Collect this list of yesterday's top 100 authors.
    For each author, gather their unique ebook formats.
    """
    def __init__(self):
        self._configure_logging()
        self.base_url = "http://www.gutenberg.org"
        self.url_popular = "/browse/scores/top"

    def _configure_logging(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler = logging.FileHandler(f"{self.__class__.__name__}.log")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def run(self):
        """Execute the scraper
        Scraper downloads list of top books.
        Each list book is checked for a plaintext copy.
        If plain text exists, download the book.
        """
        # Download urls
        top_books = self.get_top_books(f"{self.base_url}{self.url_popular}")
        with ThreadPool(processes=10) as pool:
            results = pool.map(self.download_ebook_plain_text, top_books)
            pool.close()
            pool.join()
        return results

    def get_top_books(self, url: str) -> Generator[Dict[str, str], None, None]:
        """
        Scrap top ebooks from Gutneburg's top link
        :param url:
        :return:
        """
        self.logger.info("Collecting top book info")
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", href=True)

        for link in links:
            if "ebook" not in link["href"]:
                continue
            yield {
                "title": link.text,
                "href": f"{self.base_url}{link['href']}"
            }

    def download_ebook_plain_text(self, ebook_info: Dict) -> Union[bool, None]:
        """
        Download the book HTML page.
        Parse all anchros with href.
        If plain text, download
        :param ebook_info:
        :return:
        """
        response = requests.get(ebook_info["href"])
        soup = BeautifulSoup(response.content, "html.parser")
        links = soup.find_all("a", href=True)

        # Get the download link for plain text
        link_download_text = None
        for link in links:
            if link["href"][-3:] != "txt":
                continue
            link_download_text = link["href"]
            break

        # Nothing to do here
        if link_download_text is None:
            return

        # Download the response and persist
        self.logger.info(f"Downloading: {ebook_info['title']}")
        response = requests.get(f"{self.base_url}{link_download_text}")
        filepath = f"/tmp/{ebook_info['title']}.txt"
        with open(filepath, "wb") as fout:
            fout.write(response.content)
        return True



if __name__ == "__main__":
    scraper_guetenburg = ScraperGutenburg()
    results = scraper_guetenburg.run()
    for result in results:
        print(result)

        



