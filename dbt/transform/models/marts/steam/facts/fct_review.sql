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
),

dim_language as (
    SELECT *
    FROM {{ ref('dim_language') }}
),

dim_game as (
    SELECT *
    FROM {{ ref('dim_game') }}
),

reviews_enriched as (
    SELECT 
        r.recommendationid,
        r.timestamp_created,
        r.timestamp_updated,
        l.language_key,
        u.user_key,
        g.game_key,
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
    LEFT JOIN dim_user u ON r.author_steamid = u.user_id
    LEFT JOIN dim_language l ON r.language = l.language
    LEFT JOIN dim_game g ON r.appid = g.game_key
),
added_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'recommendationid',
            'timestamp_created',
            'timestamp_updated',
            'language_key',
            'user_key',
            'game_key',
            'user_last_played',
            'user_playtime_at_review',
            'review',
            'voted_up',
            'votes_up',
            'votes_funny',
            'weighted_vote_score',
            'written_during_early_access',
            'comment_count',
            'author_playtime_forever',
            'author_playtime_last_two_weeks',
            'steam_purchase'
        ]) }} AS review_key,
        re.*
    FROM reviews_enriched re
)
SELECT *
FROM added_surrogate_key