{{ config(
    unique_key='developer_key'
) }}

WITH exploded AS (
    SELECT explode(developers) AS developer
    FROM silver.games
),
deduplicated AS (
    SELECT DISTINCT developer
    FROM exploded
)

SELECT
    sha2(developer, 256) AS developer_key,
    developer developer_name
FROM deduplicated