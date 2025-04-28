import os
import psycopg2
import psycopg2.extras
import logging
import time

class PostgresClient:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("PG_DATABASE"),
                user=os.getenv("PG_USER"),
                password=os.getenv("PG_PASSWORD"),
                host=os.getenv("PG_HOST"),
                port=os.getenv("PG_PORT"),
                sslmode="require" if os.getenv("PG_SSL").lower() == "true" else "disable"
            )
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            logging.info("Connected to PostgreSQL database!")
        except Exception as e:
            logging.error("Failed to connect to PostgreSQL: %s", str(e))
            os._exit(1)

    def save_site(self, url, html):
        """
        Save crawled site into database
        """
        try:
            query = """
                INSERT INTO crawled_sites (url, html, crawled_at)
                VALUES (%s, %s, %s)
            """
            self.cursor.execute(query, (url, html, int(time.time())))
            self.conn.commit()
            logging.info("Saved site %s to database", url)
        except Exception as e:
            logging.error("Failed to save site %s: %s", url, str(e))
            self.conn.rollback()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("PostgreSQL connection closed.")
