CREATE TABLE IF NOT EXISTS steam_catalog.bronze.steam_reviews (
    recommendationid INT,
    appid INT,
    game STRING,
    author_steamid LONG,
    author_num_games_owned INT,
    author_num_reviews INT,
    author_playtime_forever INT,
    author_playtime_last_two_weeks INT,
    author_playtime_at_review INT,
    author_last_played INT,
    language STRING,
    review STRING,
    timestamp_created INT,
    timestamp_updated INT,
    voted_up INT,
    votes_up INT,
    votes_funny INT,
    weighted_vote_score DOUBLE,
    comment_count INT,
    steam_purchase INT,
    received_for_free INT,
    written_during_early_access INT,
    hidden_in_steam_china INT,
    steam_china_location STRING,
    created_day DATE
)
USING iceberg
PARTITIONED BY (created_day)
LOCATION '${LAKEHOUSE_URL}/bronze.db/steam_reviews'
TBLPROPERTIES (
    'format-version' = '2'
);