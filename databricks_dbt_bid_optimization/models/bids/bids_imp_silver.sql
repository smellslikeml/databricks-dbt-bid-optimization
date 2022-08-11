with bronze as (
    select
        id as auction_id,
        explode(imp) as imp
    from {{ ref('bids_bronze') }}
),

get_keys as (
    select
        auction_id,
        imp.*
    from bronze
)

select
    auction_id as imp_auction_id,
    bidfloor as imp_bidfloor,
    id as imp_id,
    instl as imp_instl,
    pmp as imp_pmp,
    secure as imp_secure,
    tagid as imp_tagid,
    banner.h as imp_banner_h,
    banner.pos as imp_banner_pos,
    banner.w as imp_banner_w
from get_keys