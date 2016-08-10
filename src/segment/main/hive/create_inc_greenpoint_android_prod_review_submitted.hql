--/*
--  HIVE SCRIPT  : create_inc_greenpoint_android_prod_review_submitted.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_review_submitted). 
--*/

--  Creating a incoming hive table(inc_greenpoint_android_prod_review_submitted) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_review_submitted
(
id STRING,
received_at STRING,
uuid STRING,
anonymous_id STRING,
context_app_build STRING,
context_app_name STRING,
context_app_namespace STRING,
context_app_version STRING,
context_device_ad_tracking_enabled STRING,
context_device_advertising_id STRING,
context_device_id STRING,
context_device_manufacturer STRING,
context_device_model STRING,
context_device_name STRING,
context_device_type STRING,
context_ip STRING,
context_library_name STRING,
context_library_version STRING,
context_library_version_name STRING,
context_locale STRING,
context_network_bluetooth STRING,
context_network_carrier STRING,
context_network_cellular STRING,
context_network_wifi STRING,
context_os_name STRING,
context_os_sdk STRING,
context_os_version STRING,
context_screen_density STRING,
context_screen_density_bucket STRING,
context_screen_density_dpi STRING,
context_screen_height STRING,
context_screen_scaled_density STRING,
context_screen_width STRING,
context_timezone STRING,
context_traits_anonymous_id STRING,
context_traits_email STRING,
context_traits_user_id STRING,
context_user_agent STRING,
event STRING,
event_text STRING,
member_id STRING,
original_timestamp STRING,
sent_at STRING,
service_provider_id STRING,
timestamp STRING,
user_id STRING,
member_id_legacy STRING,
user_id40 STRING,
uuid_ts STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/segment/events/greenpoint_android_prod/incremental/daily/inc_review_submitted';