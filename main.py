import os
import logging
from dotenv import load_dotenv

from logs.setup import setup_logger
from crawl.fetcher import fetch_page
from crawl.parser import extract_links
from crawl.utils import is_valid_url
from crawl.storage import save_page

SEEDS_FILE = "configs/seeds.txt"

def load_seeds(filename):
    logging.info("loading seeds from %s", filename)

    with open(filename, 'r') as f:
        seeds = [line.strip() for line in f if line.strip()]

        logging.info("load success. will fetch from %s", seeds)
    return seeds

def main():
    load_dotenv()

    setup_logger()

    seeds = load_seeds(SEEDS_FILE)
    seen_urls = set()
    url_queue = seeds.copy()

    site_limit = int(os.getenv("SITE_LIMIT"))

    while url_queue and len(seen_urls) < site_limit:
        url = url_queue.pop(0)
        if url in seen_urls:
            continue

        logging.info(f"crawling: {url}")
        html = fetch_page(url)
        if html is None:
            logging.warning(f'failed to fetch {url}')
            continue

        save_page(url, html)
        seen_urls.add(url)

        links = extract_links(html, url)

        for link in links:
            if is_valid_url(link) and link not in seen_urls:
                url_queue.append(link)

if __name__ == '__main__':
    main()