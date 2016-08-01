--/*
--  HIVE SCRIPT  : create_inc_screens.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_screens). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_inc_screens.hql \
--     --hivevar SEGMENT_INCOMING_DB="${SEGMENT_INCOMING_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 
--*/

--  Creating a incoming hive table(inc_store_page_loaded) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_INCOMING_DB}.inc_store_page_loaded
(
id STRING,
received_at STRING,
uuid BIGINT,
anonymous_id STRING,
context_app_build BIGINT,
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
context_os_sdk BIGINT,
context_os_version STRING,
context_screen_density BIGINT,
context_screen_density_bucket STRING,
context_screen_density_dpi BIGINT,
context_screen_height BIGINT,
context_screen_scaled_density BIGINT,
context_screen_width BIGINT,
context_timezone STRING,
context_traits_anonymous_id STRING,
context_traits_email STRING,
context_traits_user_id STRING,
context_user_agent STRING,
member_id STRING,
name STRING,
op_system STRING,
original_STRING STRING,
sent_at STRING,
service_provider_id BIGINT,
STRING STRING,
user_id STRING,
user_id40 BIGINT,
member_id_legacy BIGINT,
uuid_ts STRING
)
PARTITIONED BY (edh_bus_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "\u0001",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_SEGMENT}/greenpoint_android_prod/incremental/daily/inc_screens';