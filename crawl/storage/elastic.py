import os
import json
import logging
from elasticsearch import Elasticsearch

class ElasticSearchClient:
    def __init__(self):
        self.site_index = os.getenv("ES_SITE_INDEX")
        self.client = None

    def connect(self):
        es_urls = json.loads(os.getenv("ES_URL"))
        es_usr = os.getenv("ES_USER")
        es_pwd = os.getenv("ES_PASSWORD")
        es_crt = os.getenv("ES_CERT")

        self.client = Elasticsearch(
            hosts=es_urls,
            ca_certs=es_crt if es_crt else None,
            basic_auth=(es_usr, es_pwd)
        )

        if self.client.ping():
            logging.info("Connected to Elasticsearch on %s", es_urls)
        else:
            logging.error("Failed to connect to Elasticsearch on %s", es_urls)
            os._exit(1)

    def save_site(self, doc):
        self.client.index(index=self.site_index, document=doc)

    def close(self):
        self.client.close()
        logging.info("Elastic Search connection closed.")
