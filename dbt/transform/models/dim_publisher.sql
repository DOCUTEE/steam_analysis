{{ config(
    unique_key='publisher_key'
) }}

WITH exploded AS (
    SELECT explode(publishers) AS publisher
    FROM {{ source('silver', 'games') }}
),
deduplicated AS (
    SELECT DISTINCT publisher
    FROM exploded
)

SELECT
    sha2(publisher, 256) AS publisher_key,
    publisher publisher_name
FROM deduplicated
