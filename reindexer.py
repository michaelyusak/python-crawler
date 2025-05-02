import os
import logging
import time

from storage.elastic import ElasticSearchClient
from storage.postgres import PostgresClient
from storage.local import save_page
from utils.parser import html_to_es_doc

class ReIndexer:
    def __init__(self, es_client: ElasticSearchClient, pg_client: PostgresClient):
        self.es_client = es_client
        self.pg_client = pg_client

        self.disable_local_storage = os.getenv("DISABLE_LOCAL_STORAGE").lower() == "true"

    def reindex(self):
        hashes = self.pg_client.get_all_hash()

        done = 0
        total = len(hashes)

        for hash in hashes:
            crawled = self.pg_client.get_site_by_hash(hash)

            doc = html_to_es_doc(crawled["url"], crawled["html"])
            doc["hash"] = hash

            if not self.disable_local_storage:
                save_page(crawled["url"], crawled["html"])

            self.es_client.save_site(doc)

            done += 1
            logging.info("%s completed | %s targeted", done, total)

            time.sleep(0.5)

