--/*
--  HIVE SCRIPT  : create_inc_greenpoint_production_writes_an_sp_review.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_greenpoint_production_writes_an_sp_review). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_inc_greenpoint_production_writes_an_sp_review.hql \
     --hivevar SEGMENT_INCOMING_DB="${SEGMENT_INCOMING_DB}" \
     --hivevar S3_BUCKET="${S3_BUCKET}" \
     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 
--*/

--  Creating a incoming hive table(inc_greenpoint_production_writes_an_sp_review) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_INCOMING_DB}.inc_greenpoint_production_writes_an_sp_review
(
id	STRING,
received_at	STRING,
uuid STRING,
anonymous_id STRING,
context_app_build STRING,
context_app_name STRING,
context_app_version	STRING,
context_device_ad_tracking_enabled	STRING,
context_device_idfa	STRING,
context_device_idfv	STRING,
context_device_manufacturer	STRING,
context_device_model	STRING,
context_device_type	STRING,
context_ip	STRING,
context_library_name STRING,
context_library_version	STRING,
context_locale	STRING,
context_network_bluetooth	STRING,
context_network_carrier	STRING,
context_network_wifi	STRING,
context_os_name	STRING,
context_os_version	STRING,
context_screen_height STRING,
context_screen_width STRING,
event	STRING,
event_text	STRING,
original_timestamp	STRING,
review_origin STRING,
sent_at	STRING,
spid	STRING,
spidreview	STRING,
timestamp	STRING,
user_id	STRING,
member_id_legacy	STRING,
user_id40	STRING,
uuid_ts	STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_SEGMENT}/greenpoint_production/incremental/daily/inc_greenpoint_production_writes_an_sp_review';