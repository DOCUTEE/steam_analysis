CREATE TABLE IF NOT EXISTS steam_catalog.bronze.games (
    appid INT,
    categories ARRAY<STRING>,
    created_at DOUBLE,
    detailed_description STRING,
    developers ARRAY<STRING>,
    genres ARRAY<STRING>,
    is_free BOOLEAN,
    name STRING,
    platforms STRUCT<
        windows: BOOLEAN, 
        mac: BOOLEAN, 
        linux: BOOLEAN
    >,
    price_overview STRUCT<
        currency: STRING,
        initial: DOUBLE,
        final: DOUBLE,
        discount_percent: INT,
        initial_formatted: STRING,
        final_formatted: STRING
    >,
    publishers ARRAY<STRING>,
    release_date STRUCT<coming_soon: BOOLEAN, date: STRING>,
    required_age INT,
    short_description STRING,
    type STRING
)
USING iceberg
LOCATION '${LAKEHOUSE_URL}/bronze.db/games'
TBLPROPERTIES (
    'format-version' = '2'
);