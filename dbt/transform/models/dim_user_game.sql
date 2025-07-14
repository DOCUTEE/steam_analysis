{{ config(
    unique_key=['timestamp', 'user_key', 'game_key']
)}}

WITH
joined AS (
    SELECT * FROM
    silver.steam_reviews AS reviews
    JOIN
    {{ ref('dim_user') }} AS user
    ON reviews.author_steamid = user.user_key
    JOIN
    {{ ref('dim_game') }} AS game
    ON reviews.appid = game.game_key
)
SELECT
    unix_timestamp(joined.created_day) AS timestamp,
    user_key,
    game_key,
    author_playtime_forever AS user_playtime_forever,
    author_playtime_last_two_weeks AS user_playtime_last_two_weeks,
    author_last_played AS user_last_played,
    steam_purchase,
    received_for_free
FROM joined
