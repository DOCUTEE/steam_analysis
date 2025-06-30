CREATE TABLE IF NOT EXISTS steam_catalog.silver.steam_reviews (
    recommendationid STRING,
    appid INT,
    game STRING,
    author_steamid STRING,
    author_num_games_owned INT,
    author_num_reviews INT,
    author_playtime_forever INT,
    author_playtime_last_two_weeks INT,
    author_playtime_at_review INT,
    author_last_played INT,
    language STRING,
    review STRING,
    timestamp_created TIMESTAMP,
    timestamp_updated TIMESTAMP,
    voted_up BOOLEAN,
    votes_up INT,
    votes_funny INT,
    weighted_vote_score DOUBLE,
    comment_count INT,
    steam_purchase BOOLEAN, 
    received_for_free BOOLEAN,
    written_during_early_access BOOLEAN,
    hidden_in_steam_china BOOLEAN,
    steam_china_location STRING,
    created_day DATE
)
USING iceberg
Partitioned BY (created_day)
LOCATION '${LAKEHOUSE_URL}/silver.db/steam_reviews'
TBLPROPERTIES (
    'format-version'='2'
)