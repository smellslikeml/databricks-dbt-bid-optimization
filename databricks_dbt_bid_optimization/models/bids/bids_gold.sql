-- Databricks notebook source

SELECT *, round(rand()+0.1) as in_view
  FROM (
     SELECT * FROM  {{ ref('bids_device_silver')}} A
     LEFT JOIN {{ ref('bids_imp_silver') }} B ON A.device_auction_id = B.imp_auction_id ) D
     LEFT JOIN {{ ref('bids_site_silver') }} C ON D.device_auction_id = C.site_auction_id

