-- Databricks notebook source
select
    id as site_auction_id,
    regs.ext.gdpr as site_gdpr_status,
    site.cat as site_cat,
    site.content as site_content,
    site.domain as site_domain,
    site.id as site_id,
    site.keywords as site_keywords,
    site.name as site_name,
    site.page as site_page,
    site.pagecat as site_pagecat,
    site.privacypolicy as site_privacypolicy,
    site.publisher.id as site_publisher_id,
    site.ref as site_ref
from {{ ref('bids_bronze') }}

