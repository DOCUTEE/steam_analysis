{{ config(
    unique_key=['user_key']
)}}

SELECT
    unix_timestamp(s.review_day) as timestamp,
    s.author_steamid as user_key,
    s.author_num_games_owned as user_num_games_owned,
    s.author_num_reviews as user_num_reviews
FROM {{ ref('stg_steam__reviews') }} as s
