{{ config(
    unique_key='game_key'
) }}


SELECT
    appid game_key,
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
    required_age required_age
FROM {{ source('silver', 'games') }}
