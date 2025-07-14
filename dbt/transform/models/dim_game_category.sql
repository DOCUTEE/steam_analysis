{{ config(
    unique_key=['category_key', 'game_key']
) }}

WITH exploded AS (
    SELECT
        explode(categories) AS category,
        appid AS game_key
    FROM {{ source('silver', 'games') }}
)

SELECT DISTINCT p.category_key, g.game_key
FROM exploded
JOIN {{ ref('dim_category') }} p
ON exploded.category = p.category_name
JOIN {{ ref('dim_game') }} g
ON exploded.game_key = g.game_key
