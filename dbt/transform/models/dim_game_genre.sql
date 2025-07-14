{{ config(
    unique_key=['genre_key', 'game_key']
) }}

WITH exploded AS (
    SELECT
        explode(genres) AS genre,
        appid AS game_key
    FROM {{ source('silver', 'games') }}
)

SELECT DISTINCT p.genre_key, g.game_key
FROM exploded
JOIN {{ ref('dim_genre') }} p
ON exploded.genre = p.genre_name
JOIN {{ ref('dim_game') }} g
ON exploded.game_key = g.game_key
