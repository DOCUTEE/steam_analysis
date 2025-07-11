{{ config( 
    unique_key='user_key', 
)}}

SELECT 
    s.author_steamid as user_key, 
    s.author_num_games_owned as user_num_games_owned,
    s.author_num_reviews as user_num_reviews
FROM silver.steam_reviews as s 