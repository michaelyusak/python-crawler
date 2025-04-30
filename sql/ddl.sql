\c python_crawler_db

CREATE TABLE crawled_sites (
    url VARCHAR,
    html VARCHAR,
    hash VARCHAR,
    crawled_at BIGINT
);