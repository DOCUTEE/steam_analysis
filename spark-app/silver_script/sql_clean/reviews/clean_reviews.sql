WITH reviews_cleaned AS (
    SELECT 
        recommendationid,
        appid,
        game,
        author_steamid,
        COALESCE(author_num_games_owned, 0) AS author_num_games_owned,
        COALESCE(author_num_reviews, 0) AS author_num_reviews,
        COALESCE(author_playtime_forever, 0) AS author_playtime_forever,
        COALESCE(author_playtime_last_two_weeks, 0) AS author_playtime_last_two_weeks,
        COALESCE(author_playtime_at_review, 0) AS author_playtime_at_review,
        COALESCE(author_last_played, 0) AS author_last_played,
        COALESCE(language, 'Unknown') AS language,
        review,
        TO_TIMESTAMP(CAST(timestamp_created AS BIGINT)) AS timestamp_created,
        CASE 
            WHEN timestamp_updated IS NULL THEN TO_TIMESTAMP(CAST(timestamp_created AS BIGINT))
            ELSE TO_TIMESTAMP(CAST(timestamp_updated AS BIGINT))
        END AS timestamp_updated,
        CAST(COALESCE(voted_up, 0) AS BOOLEAN) AS voted_up,
        COALESCE(votes_up, 0) AS votes_up,
        COALESCE(votes_funny, 0) AS votes_funny,
        COALESCE(weighted_vote_score, 0) AS weighted_vote_score,
        COALESCE(comment_count, 0) AS comment_count,
        CAST(COALESCE(steam_purchase, 0) AS BOOLEAN) AS steam_purchase,
        CAST(COALESCE(received_for_free, 0) AS BOOLEAN) AS received_for_free,
        CAST(COALESCE(written_during_early_access, 0) AS BOOLEAN) AS written_during_early_access,
        CAST(COALESCE(hidden_in_steam_china, 0) AS BOOLEAN) AS hidden_in_steam_china,
        COALESCE(steam_china_location, 'Unknown') AS steam_china_location,
        created_day
    FROM steam_catalog.bronze.steam_reviews
    WHERE 
        to_date(from_unixtime(timestamp_created)) = DATE('${cleaning_day}')
        AND author_steamid IS NOT NULL
        AND review IS NOT NULL AND review != ''
        AND recommendationid IS NOT NULL
        AND appid IS NOT NULL
        AND timestamp_created IS NOT NULL
)

MERGE INTO steam_catalog.silver.steam_reviews AS target
USING reviews_cleaned AS source
ON target.recommendationid = source.recommendationid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *