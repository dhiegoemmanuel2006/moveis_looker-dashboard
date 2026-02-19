CREATE TABLE IF NOT EXISTS bronze.movies(
    adult VARCHAR,
    backdrop_path VARCHAR,
    genre_ids VARCHAR,
    id VARCHAR,
    original_language VARCHAR,
    original_title VARCHAR,
    overview VARCHAR,
    popularity VARCHAR,
    poster_path VARCHAR,
    release_date VARCHAR,
    title VARCHAR,
    video VARCHAR,
    vote_average VARCHAR,
    vote_count VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
