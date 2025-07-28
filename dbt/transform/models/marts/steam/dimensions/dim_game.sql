{{ config(
    unique_key='game_key'
) }}


WITH stg_steam__games AS (
    SELECT *
    FROM {{ ref('stg_steam__games') }}
),
soft_transformed_games AS (
    SELECT
        appid,
        name game_name,
        type,
        short_description,
        detailed_description,
        is_free,
        price_overview.initial initial_price,
        price_overview.discount_percent discount_percent,
        price_overview.final final_price,
        platforms.windows windows,
        platforms.mac mac,
        platforms.linux linux,
        unix_timestamp(created_at) created_at,
        release_date.coming_soon coming_soon,
        unix_timestamp(release_date.date) release_date,
        required_age required_age, 
        array_join(categories, ', ') AS category_name,
        array_join(developers, ', ') AS developer_name,
        array_join(genres, ', ') AS genre_name,
        array_join(publishers, ', ') AS publisher_name
    FROM stg_steam__games
),
added_surrogate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'appid',
            'game_name',
            'type',
            'short_description',
            'detailed_description',
            'is_free',
            'initial_price',
            'discount_percent',
            'final_price',
            'windows',
            'mac',
            'linux',
            'created_at',
            'coming_soon',
            'release_date',
            'required_age',
            'category_name',
            'developer_name',
            'genre_name',
            'publisher_name'
        ]) }} AS game_key,
        stg.*
    FROM soft_transformed_games AS stg
)

SELECT * 
FROM added_surrogate_key
