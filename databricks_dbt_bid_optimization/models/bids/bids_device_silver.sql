
-- Use the `ref` function to select from other models

select
id device_auction_id,
device.carrier device_carrier,
device.connectiontype device_connectiontype,
device.devicetype device_devicetype,
device.dnt device_dnt,
device.dpidmd5 device_dpidmd5,
device.dpidsha1 device_dpidsha1,
device.h device_h,
device.ifa device_ifa,
device.ip device_ip,
device.ipv6 device_ipv6,
device.js device_js,
device.language device_language,
device.make device_make,
device.model device_model,
device.os device_os,
device.osv device_osv,
device.ua device_ua,
device.w device_w,
device.geo.accuracy device_accuracy,
device.geo.city device_city,
device.geo.country device_country,
device.geo.lat device_lat,
device.geo.lon device_lon,
device.geo.metro device_metro,
device.geo.region device_region,
device.geo.type device_type,
device.geo.zip device_zip
from {{ ref('bids_bronze') }}
