CREATE VIEW gold.vw_movies AS
SELECT
    movie_id,
    title,
    original_language,
    is_adult,
    release_date,
    CASE 
        WHEN vote_average > 8 THEN 'Alta'
        WHEN vote_average BETWEEN 5 AND 8 THEN 'Média'
        ELSE 'Baixa'
    END AS categoria_avaliacao,
    vote_count,
    popularity
FROM silver.movies
WHERE vote_count > 100; -- Filtro de relevância para o negócio