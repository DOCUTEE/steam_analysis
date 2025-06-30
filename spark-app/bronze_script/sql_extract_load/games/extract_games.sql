With steam_games as (
    SELECT
        appid,
        categories,
        created_at,
        detailed_description,
        developers,
        genres,
        is_free,
        name,
        platforms,
        STRUCT(
            price_overview.currency AS currency,
            CAST(price_overview.initial AS DOUBLE) AS initial,
            CAST(price_overview.final AS DOUBLE) AS final,
            CAST(price_overview.discount_percent AS INT) AS discount_percent,
            price_overview.initial_formatted AS initial_formatted,
            price_overview.final_formatted AS final_formatted
        ) AS price_overview,
        publishers,
        release_date,
        CAST(required_age AS INT) AS required_age,
        short_description,
        type
    FROM original_games
)
MERGE INTO steam_catalog.bronze.games AS target
USING steam_games AS source
ON target.appid = source.appid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *