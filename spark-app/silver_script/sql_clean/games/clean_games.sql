WITH game_cleaned AS (
    SELECT 
        appid,
        CASE WHEN categories IS NULL THEN ARRAY() ELSE categories END AS categories,
        TO_TIMESTAMP(created_at) AS created_at,
        CASE WHEN detailed_description IS NULL THEN '' ELSE detailed_description END AS detailed_description,
        CASE WHEN developers IS NULL THEN ARRAY() ELSE developers END AS developers,
        CASE WHEN genres IS NULL THEN ARRAY() ELSE genres END AS genres,
        CASE WHEN is_free IS NULL THEN FALSE ELSE is_free END AS is_free,
        name,
        STRUCT(
            CASE WHEN platforms.windows IS NULL THEN FALSE ELSE platforms.windows END,
            CASE WHEN platforms.mac IS NULL THEN FALSE ELSE platforms.mac END,
            CASE WHEN platforms.linux IS NULL THEN FALSE ELSE platforms.linux END
        ) AS platforms,
        STRUCT(
            'VND' AS currency,
            CASE 
                WHEN price_overview.currency = 'USD' THEN price_overview.initial * 24000
                WHEN price_overview.currency = 'EUR' THEN price_overview.initial * 26000
                ELSE price_overview.initial
            END AS initial,
            CASE 
                WHEN price_overview.currency = 'USD' THEN price_overview.final * 24000
                WHEN price_overview.currency = 'EUR' THEN price_overview.final * 26000
                ELSE price_overview.final
            END AS final,
            COALESCE(price_overview.discount_percent, 0) AS discount_percent,
            CAST(
                CASE 
                    WHEN price_overview.currency = 'USD' THEN price_overview.initial * 24000
                    WHEN price_overview.currency = 'EUR' THEN price_overview.initial * 26000
                    ELSE price_overview.initial
                END AS STRING
            ) AS initial_formatted,
            CONCAT(
                CAST(
                    CASE 
                        WHEN price_overview.currency = 'USD' THEN price_overview.final * 24000
                        WHEN price_overview.currency = 'EUR' THEN price_overview.final * 26000
                        ELSE price_overview.final
                    END AS BIGINT
                ), '₫'
            ) AS final_formatted
        ) AS price_overview,
        CASE WHEN publishers IS NULL THEN ARRAY() ELSE publishers END AS publishers,
        STRUCT(
            CASE WHEN release_date.coming_soon IS NULL THEN FALSE ELSE release_date.coming_soon END,
            CASE 
                WHEN release_date.date IS NULL THEN TO_DATE('1970-01-01')
                ELSE TO_DATE(replace(release_date.date, ',', ''), 'd MMM yyyy')
            END
        ) AS release_date,
        CASE WHEN required_age IS NULL THEN 0 ELSE required_age END AS required_age,
        CASE WHEN short_description IS NULL THEN '' ELSE short_description END AS short_description,
        CASE WHEN type IS NULL THEN 'game' ELSE type END AS type
    FROM steam_catalog.bronze.games
    WHERE appid IS NOT NULL
      AND created_at IS NOT NULL
      AND name IS NOT NULL
)
MERGE INTO steam_catalog.silver.games AS target
USING game_cleaned AS source
ON target.appid = source.appid
WHEN MATCHED THEN 
    UPDATE SET *
WHEN NOT MATCHED THEN 
    INSERT *
