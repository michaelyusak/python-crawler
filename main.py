import os
import logging
from dotenv import load_dotenv
import urllib.robotparser as robot_parser

from logs.setup import setup_logger
from crawl.fetcher import fetch_page
from crawl.parser import extract_links, html_to_es_doc
from crawl.utils import is_valid_url
from crawl.storage.elastic import ElasticSearchClient
from crawl.storage.postgres import PostgresClient
from crawl.storage.local import save_page

SEEDS_FILE = "configs/seeds.txt"

def load_seeds(filename):
    logging.info("loading seeds from %s", filename)

    with open(filename, 'r') as f:
        seeds = [line.strip() for line in f if line.strip()]

        logging.info("load success. will fetch from %s", seeds)
    return seeds

def main():
    load_dotenv()

    user_agent = os.getenv("USER_AGENT")

    setup_logger()

    es_client = ElasticSearchClient()
    es_client.connect()

    pg_client = PostgresClient()
    pg_client.connect()

    seeds = load_seeds(SEEDS_FILE)
    seen_urls = set()
    url_queue = seeds.copy()

    site_limit = int(os.getenv("SITE_LIMIT"))

    disable_local_storage = os.getenv("DISABLE_LOCAL_STORAGE").lower() == "true"

    rp = robot_parser.RobotFileParser()

    while url_queue and len(seen_urls) < site_limit:
        url = url_queue.pop(0)
        if url in seen_urls:
            continue

        rp.set_url(url + "/robots.txt")

        rp.read()

        if not rp.can_fetch(user_agent, url):
            continue

        logging.info(f"crawling: {url}")
        html = fetch_page(url)
        if html is None:
            logging.warning(f'failed to fetch {url}')
            continue

        doc = html_to_es_doc(url, html)

        if not disable_local_storage:
            save_page(url, html)

        pg_client.save_site(url, html)
        es_client.save_site(doc=doc)

        seen_urls.add(url)

        links = extract_links(html, url)

        for link in links:
            if is_valid_url(link) and link not in seen_urls:
                url_queue.append(link)

    es_client.close()
    pg_client.close()

if __name__ == '__main__':
    main()