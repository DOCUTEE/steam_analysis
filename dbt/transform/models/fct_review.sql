{{ config(
    unique_key='review_key'
)}}

with joined as (
    select
    *
    from
        {{ source('silver', 'steam_reviews') }} fct
        join
        {{ ref('dim_language') }} dl
        on fct.language = dl.language
        join
        {{ ref('dim_user_game') }} dug
        on fct.author_steamid = dug.user_key and fct.appid = dug.game_key
)

select
    recommendationid review_key,
    review review,
    unix_timestamp(timestamp_created) timestamp_created,
    unix_timestamp(timestamp_updated) timestamp_updated,
    language_key,
    voted_up,
    votes_up,
    votes_funny,
    weighted_vote_score,
    written_during_early_access,
    author_steamid user_key,
    appid game_key,
    author_playtime_at_review user_playtime_at_review,
    comment_count
from joined
