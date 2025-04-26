import logging

from logs.setup import setup_logger
from crawl.fetcher import fetch_page

SEEDS_FILE = "configs/seeds.txt"

def load_seeds(filename):
    logging.info("loading seeds from %s", filename)

    with open(filename, 'r') as f:
        seeds = [line.strip() for line in f if line.strip()]

        logging.info("load success. will fetch from %s", seeds)
    return seeds

def main():
    setup_logger()

    seeds = load_seeds(SEEDS_FILE)
    seen_urls = set()
    url_queue = seeds.copy()

    while url_queue and len(seen_urls) < 10000:
        url = url_queue.pop(0)
        if url in seen_urls:
            continue

        logging.info(f"crawling: {url}")
        html = fetch_page(url)
        if html is None:
            logging.warning(f'failed to fetch {url}')
            continue

        print(html)

    

if __name__ == '__main__':
    main()