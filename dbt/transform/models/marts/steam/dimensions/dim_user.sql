{{ config(
    materialized='incremental',
    unique_key='user_key',
    incremental_strategy='merge',
    file_format='iceberg'
) }}

WITH user_data AS (
    SELECT
        distinct s.author_steamid as user_id,
        s.author_num_games_owned as user_num_games_owned,
        s.author_num_reviews as user_num_reviews
    FROM {{ ref('stg_steam__reviews') }} as s
),
added_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'user_id', 
            'user_num_games_owned', 
            'user_num_reviews'
        ]) }} AS user_key,
        user_data.*
    FROM user_data
)
SELECT *
FROM added_surrogate_key



