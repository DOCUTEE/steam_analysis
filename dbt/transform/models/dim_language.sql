{{ config(
    unique_key='language_key'
)}}

with deduplicated as (
    select
        distinct language
    from {{ source('silver', 'steam_reviews') }}
)

select
    sha2(language, 256) as language_key,
    language language
from deduplicated
