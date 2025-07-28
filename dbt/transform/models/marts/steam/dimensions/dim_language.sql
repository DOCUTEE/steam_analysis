{{ config(
    unique_key='language_key'
)}}

with deduplicated_language as (
    select
        distinct language
    from {{ ref('stg_steam__reviews') }}
)
select
    {{dbt_utils.generate_surrogate_key(['language'])}} as language_key,
    language language
from deduplicated_language
