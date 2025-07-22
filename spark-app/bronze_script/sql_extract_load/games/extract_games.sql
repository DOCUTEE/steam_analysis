MERGE INTO steam_catalog.bronze.games AS target
USING new_games AS source
ON target.appid = source.appid
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *