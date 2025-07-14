{{ config(
    unique_key='category_key'
) }}

WITH exploded AS (
    SELECT explode(categories) AS category
    FROM {{ source('silver', 'games') }}
),
deduplicated AS (
    SELECT DISTINCT category
    FROM exploded
)

SELECT
    sha2(category, 256) AS category_key,
    category category_name
FROM deduplicated
