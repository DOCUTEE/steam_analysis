{{ config(
    unique_key=['developer_key', 'game_key']
) }}

WITH exploded AS (
    SELECT
        explode(developers) AS developer,
        appid AS game_key
    FROM {{ source('silver', 'games') }}
)

SELECT DISTINCT p.developer_key, g.game_key
FROM exploded
JOIN {{ ref('dim_developer') }} p
ON exploded.developer = p.developer_name
JOIN {{ ref('dim_game') }} g
ON exploded.game_key = g.game_key
