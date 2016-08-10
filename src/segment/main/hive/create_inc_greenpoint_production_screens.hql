--/*
--  HIVE SCRIPT  : create_inc_angieslistweb_prod_purchased_cart.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_greenpoint_production_screens). 
--*/

--  Creating a incoming hive table(inc_greenpoint_production_screens) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_screens
(
id STRING,
received_at STRING,
uuid STRING,
anonymous_id STRING,
app_version STRING,
category STRING,
context_app_build STRING,
context_app_name STRING,
context_app_version STRING,
context_device_ad_tracking_enabled STRING,
context_device_idfa STRING,
context_device_idfv STRING,
context_device_manufacturer STRING,
context_device_model STRING,
context_device_type STRING,
context_ip STRING,
context_library_name STRING,
context_library_version STRING,
context_locale STRING,
context_network_bluetooth STRING,
context_network_carrier STRING,
context_network_wifi STRING,
context_os_name STRING,
context_os_version STRING,
context_screen_height STRING,
context_screen_width STRING,
count STRING,
coupon STRING,
date STRING,
deal_id STRING,
is_logged_in STRING,
keyword STRING,
market_id STRING,
member_id STRING,
name STRING,
offer_item_id STRING,
offer_item_name STRING,
op_system STRING,
original_timestamp STRING,
phone_model STRING,
reason STRING,
sent_at STRING,
sort_by STRING,
sp_id STRING,
specific_screen STRING,
spid STRING,
spidview STRING,
timestamp STRING,
user_id STRING,
zipcode STRING,
user_id40 STRING,
member_id_legacy STRING,
uuid_ts STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/segment/events/greenpoint_production/incremental/daily/inc_screens';