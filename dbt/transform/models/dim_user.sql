{{ config(
    unique_key=['timestamp', 'user_key']
)}}

SELECT
    unix_timestamp(s.created_day) as timestamp,
    s.author_steamid as user_key,
    s.author_num_games_owned as user_num_games_owned,
    s.author_num_reviews as user_num_reviews
FROM {{ source('silver', 'steam_reviews') }} as s
