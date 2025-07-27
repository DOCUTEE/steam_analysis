{{ config(
    unique_key='language_key'
)}}

with deduplicated as (
    select
        distinct language
    from {{ ref('stg_steam__reviews') }}
)

select
    sha2(language, 256) as language_key,
    language language
from deduplicated
