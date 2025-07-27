{{ config(
    unique_key='review_key'
)}}

WITH stg_steam__reviews as (
    SELECT *
    FROM {{ ref('stg_steam__reviews') }}
),

dim_user as (
    SELECT *
    FROM {{ ref('dim_user') }}
)

dim_time as (
    SELECT *
    FROM {{ ref('dim_time') }}
)

dim_language as (
    SELECT *
    FROM {{ ref('dim_language') }}
)

dim_game as (
    SELECT *
    FROM {{ ref('dim_game') }}
)

reviews_enriched as (
    SELECT 
        r.recommendationid AS review_key,
        r.timestamp_created,
        r.timestamp_updated,
        l.language_key AS language_key,
        r.author_steamid AS user_key,
        g.game_key AS game_key,
        r.author_last_played as user_last_played,
        r.author_playtime_at_review as user_playtime_at_review,
        r.review,
        r.voted_up,
        r.votes_up,
        r.votes_funny,
        r.weighted_vote_score,
        r.written_during_early_access,
        r.comment_count,
        r.author_playtime_forever,
        r.author_playtime_last_two_weeks,
        r.steam_purchase
    FROM stg_steam__reviews r
    LEFT JOIN dim_user u ON r.author_steamid = u.user_key
    LEFT JOIN dim_language l ON r.language = l.language
    LEFT JOIN dim_time t ON DATE(r.timestamp_created) = t.review_day
    LEFT JOIN dim_game g ON r.appid = g.game_key
)