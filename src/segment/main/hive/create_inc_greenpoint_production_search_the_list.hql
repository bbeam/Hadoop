--/*
--  HIVE SCRIPT  : create_inc_greenpoint_production_search_the_list.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_greenpoint_production_search_the_list). 

--  Creating a incoming hive table(inc_greenpoint_production_search_the_list) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_search_the_list
(
id STRING ,
received_at STRING ,
uuid STRING ,
anonymous_id STRING ,
context_app_build STRING ,
context_app_name STRING ,
context_app_version STRING ,
context_device_ad_tracking_enabled STRING ,
context_device_idfa STRING ,
context_device_idfv STRING ,
context_device_manufacturer STRING ,
context_device_model STRING ,
context_device_type STRING ,
context_ip STRING ,
context_library_name STRING ,
context_library_version STRING ,
context_locale STRING ,
context_network_bluetooth STRING ,
context_network_carrier STRING ,
context_network_wifi STRING ,
context_os_name STRING ,
context_os_version STRING ,
context_screen_height STRING ,
context_screen_width STRING ,
event STRING ,
event_text STRING ,
original_timestamp STRING ,
sent_at STRING ,
sp_category STRING ,
sp_keyword STRING ,
timestamp STRING ,
user_id STRING ,
member_id_legacy STRING ,
user_id40 STRING ,
uuid_ts STRING 
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/segment/events/greenpoint_production/incremental/daily/inc_search_the_list';