with steam_reviews as (
    SELECT 
        recommendationid,
        appid,
        game,
        author_steamid,
        author_num_games_owned,
        author_num_reviews,
        author_playtime_forever,
        author_playtime_last_two_weeks,
        author_playtime_at_review,
        author_last_played,
        language,
        review,
        timestamp_created,
        timestamp_updated,
        voted_up,
        votes_up,
        votes_funny,
        weighted_vote_score,
        comment_count,
        steam_purchase,
        received_for_free,
        written_during_early_access,
        hidden_in_steam_china,
        steam_china_location,
        created_day
    FROM reviews
    WHERE DATE(created_day) = DATE('${extraction_day}')
)
MERGE INTO steam_catalog.bronze.steam_reviews AS target
USING steam_reviews AS source
ON target.recommendationid = source.recommendationid
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *