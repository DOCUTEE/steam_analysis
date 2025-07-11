{{ config(
    unique_key='genre_key'
) }}

WITH exploded AS (
    SELECT explode(genres) AS genre
    FROM silver.games
),
deduplicated AS (
    SELECT DISTINCT genre
    FROM exploded
)

SELECT
    sha2(genre, 256) AS genre_key,
    genre genre_name
FROM deduplicated