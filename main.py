import os
import logging
from dotenv import load_dotenv
import time

from logs.setup import setup_logger
from crawl.fetcher import fetch_page
from crawl.parser import extract_links, html_to_es_doc
from crawl.common import is_valid_url, hash
from storage.elastic import ElasticSearchClient
from storage.postgres import PostgresClient
from storage.local import save_page
from crawl.robots import safe_read_robots_txt

SEEDS_FILE = "configs/seeds.txt"

def load_seeds(filename):
    logging.info("loading seeds from %s", filename)

    with open(filename, 'r') as f:
        seeds = [line.strip() for line in f if line.strip()]

        logging.info("load success. will fetch from %s", seeds)
    return seeds

def main():
    load_dotenv(override=True)

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

    while url_queue and len(seen_urls) < site_limit:
        time.sleep(0.5)

        url = url_queue.pop(0)
        if url in seen_urls:
            continue

        rp = safe_read_robots_txt(url)

        time.sleep(0.5)

        if not rp.can_fetch(user_agent, url):
            continue

        logging.info(f"crawling: {url}")
        html = fetch_page(url)
        if html is None:
            logging.warning(f'failed to fetch {url}')
            continue

        hash_id = hash(f"{html}:{url}")

        crawled = pg_client.get_site_by_hash(hash_id)
        if crawled is not None:
            logging.info("Inequal normalized and hashed, %s - %s, saving %s", crawled["hash"], hash_id, url)
            continue

        doc = html_to_es_doc(url, html)
        doc["hash"] = hash_id

        if not disable_local_storage:
            save_page(url, html)

        pg_client.save_site(url, html, hash_id)
        es_client.save_site(doc)

        seen_urls.add(url)

        links = extract_links(html, url)

        for link in links:
            if is_valid_url(link) and link not in seen_urls:
                url_queue.append(link)

    es_client.close()
    pg_client.close()

if __name__ == '__main__':
    main()