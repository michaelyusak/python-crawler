import os
import time
import logging

from storage.elastic import ElasticSearchClient
from storage.postgres import PostgresClient
from utils.common import hash, is_valid_url
from utils.parser import html_to_es_doc, extract_links
from utils.fetcher import fetch_page
from utils.robots import safe_read_robots_txt
from storage.local import save_page

class Crawler:
    def __init__(self, es_client: ElasticSearchClient, pg_client: PostgresClient, seeds: list[str]):
        self.es_client = es_client
        self.pg_client = pg_client

        self.seeds = seeds
        self.seen_urls = set()
        self.url_queue = seeds.copy()

        self.site_limit = int(os.getenv("SITE_LIMIT"))

        self.disable_local_storage = os.getenv("DISABLE_LOCAL_STORAGE").lower() == "true"

        self.user_agent = os.getenv("USER_AGENT")


    def crawl(self):
        done = 0
        skipped = 0

        while self.url_queue and len(self.seen_urls) < self.site_limit:
            time.sleep(0.5)

            url = self.url_queue.pop(0)
            if url in self.seen_urls:
                logging.info('url %s is seen, continue', url)

                skipped += 1
                logging.info('%s completed | %s skipped | %s targeted', done, skipped, self.site_limit)
                continue

            rp = safe_read_robots_txt(url)

            time.sleep(0.5)

            if not rp.can_fetch(self.user_agent, url):
                logging.error('Robots.txt forbids fetch %s, continue', url)

                skipped += 1
                logging.info('%s completed | %s skipped | %s targeted', done, skipped, self.site_limit)
                continue

            logging.info(f"crawling: {url}")
            html = fetch_page(url)
            if html is None:
                logging.error('failed to fetch %s, continue', url)

                skipped += 1
                logging.info('%s completed | %s skipped | %s targeted', done, skipped, self.site_limit)
                continue

            hash_id = hash(f"{html}:{url}")

            crawled = self.pg_client.get_site_by_hash(hash_id)
            if crawled is not None:
                logging.info("Got crawled site by has: %s - %s, continue", hash_id, url)

                skipped += 1
                logging.info('%s completed | %s skipped | %s targeted', done, skipped, self.site_limit)
                continue

            doc = html_to_es_doc(url, html)
            doc["hash"] = hash_id

            if not self.disable_local_storage:
                save_page(url, html)

            self.pg_client.save_site(url, html, hash_id)
            self.es_client.save_site(doc)

            self.seen_urls.add(url)

            links = extract_links(html, url)

            for link in links:
                if is_valid_url(link) and link not in self.seen_urls:
                    self.url_queue.append(link)

            done += 1
            logging.info('%s completed | %s skipped | %s targeted', done, skipped, self.site_limit)