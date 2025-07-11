{{ config(
    unique_key=['publisher_key', 'game_key']
) }}

WITH exploded AS (
    SELECT 
        explode(publishers) AS publisher,
        appid AS game_key
    FROM silver.games
)

SELECT DISTINCT p.publisher_key, g.game_key
FROM exploded
JOIN {{ ref('dim_publisher') }} p
ON exploded.publisher = p.publisher_name
JOIN {{ ref('dim_game') }} g
ON exploded.game_key = g.game_key
