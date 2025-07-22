SELECT DISTINCT appid
FROM steam_catalog.bronze.steam_reviews
WHERE DATE(created_day) = DATE('${extraction_day}')