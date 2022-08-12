with bronze as (
    select
        id as auction_id,
        regs.*,
        site.*
    from {{ ref('bids_bronze') }}
),

select
    auction_id as site_auction_id,
    ext.gdpr as site_gdpr_status,
    cat as site_cat,
    domain as site_domain,
    id as site_id,
    keywords as site_keywords,
    name as site_name,
    page as site_page,
    pagecat as site_pagecat,
    privacypolicy as site_privacypolicy,
    ref as site_ref,
    publisher.id as site_publisher_id
from bronze;
