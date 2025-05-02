import os
import logging
from dotenv import load_dotenv

from logs.setup import setup_logger
from storage.elastic import ElasticSearchClient
from storage.postgres import PostgresClient
from crawler import Crawler
from reindexer import ReIndexer

SEEDS_FILE = "configs/seeds.txt"

def load_seeds(filename):
    logging.info("loading seeds from %s", filename)

    with open(filename, 'r') as f:
        seeds = [line.strip() for line in f if line.strip()]

        logging.info("load success. will fetch from %s", seeds)
    return seeds

def main():
    load_dotenv(override=True)

    setup_logger()

    es_client = ElasticSearchClient()
    es_client.connect()

    pg_client = PostgresClient()
    pg_client.connect()

    mode = os.getenv("MODE")
    match mode:
        case "crawl":
            seeds = load_seeds(SEEDS_FILE)

            crawler = Crawler(es_client, pg_client, seeds)
            crawler.crawl()

        case "reindex":
            reindexer = ReIndexer(es_client, pg_client)
            reindexer.reindex()

        case _:
            logging.error("unknown mode")

    es_client.close()
    pg_client.close()

if __name__ == '__main__':
    main()