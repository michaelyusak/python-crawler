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

    def save_site(self, url, html, hash):
        """
        Save crawled site into database
        """
        try:
            query = """
                INSERT INTO crawled_sites (url, html, hash, crawled_at)
                VALUES (%s, %s, %s, %s)
            """
            self.cursor.execute(query, (url, html, hash, int(time.time())))
            self.conn.commit()
            logging.info("[postgres][save_site] Saved site %s to database", url)
        except Exception as e:
            logging.error("[postgres][save_site] Failed to save site %s: %s", url, str(e))
            self.conn.rollback()

    def get_site(self, url):
        """
        Get stored crawled site
        """

        try:
            query = """
                SELECT url, html, hash, crawled_at
                FROM crawled_sites
                WHERE url = %s
            """
            self.cursor.execute(query, (url,))
            row = self.cursor.fetchone()
            logging.info("[postgres][get_site] Successfully fetch crawled site %s", url)
            return row
        except Exception as e:
            logging.error("[postgres][get_site] Failed to get crawled site %s: %s", url, str(e))
            return None

    def get_site_by_hash(self, hash):
        """
        Get stored crawled site
        """

        try:
            query = """
                SELECT url, html, hash, crawled_at
                FROM crawled_sites
                WHERE hash = %s
            """
            self.cursor.execute(query, (hash,))
            row = self.cursor.fetchone()
            logging.info("[postgres][get_site] Successfully fetch crawled site %s", hash)
            return row
        except Exception as e:
            logging.error("[postgres][get_site] Failed to get crawled site %s: %s", hash, str(e))
            return None
        
    def get_all_hashes(self) -> list[str]:
        """
        Get all crawled sites
        """

        try:
            query = """
                SELECT hash
                FROM crawled_sites
                ORDER 
                    BY crawled_at
            """
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            hashes = [row[0] for row in rows]
            logging.info("[postgres][get_all_hash] Successfully fetch all hash")
            return hashes
        except Exception as e:
            logging.error("[postgres][get_all_hash] Failed to get all hash: %s", str(e))
            return None

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("PostgreSQL connection closed.")
