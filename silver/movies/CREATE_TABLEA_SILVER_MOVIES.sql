CREATE TABLE IF NOT EXISTS silver.movies AS
WITH deduplicado AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id 
            ORDER BY ingestion_timestamp DESC -- Pega a ingestão mais recente
        ) AS ranking
    FROM bronze.movies
)
SELECT
    CAST(id AS INT) AS movie_id,
    title,
    original_title,
    CAST(adult AS BOOLEAN) AS is_adult,
    CAST(release_date AS DATE) AS release_date,
    CAST(popularity AS DOUBLE) AS popularity,
    CAST(vote_average AS DOUBLE) AS vote_average,
    CAST(vote_count AS INT) AS vote_count,
    original_language,
    genre_ids, -- Pode ser transformado em ARRAY dependendo da ferramenta
    overview,
    ingestion_timestamp AS silver_processed_at
FROM deduplicado
WHERE ranking = 1; -- Filtra apenas o registro único/recente