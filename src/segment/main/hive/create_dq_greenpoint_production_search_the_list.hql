--  HIVE SCRIPT  : create_dq_greenpoint_production_search_the_list.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(Dq_greenpoint_production_search_the_list). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_dq_greenpoint_production_search_the_list.hql \
--     --hivevar SEGMENT_GOLD_DB="${SEGMENT_GOLD_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 

--*/

--  Creating a DQ hive table(Dq_greenpoint_production_search_the_list) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_GOLD_DB}.dq_greenpoint_production_search_the_list
(
id  VARCHAR(254) ,
received_at TIMESTAMP ,
uuid    BIGINT ,
anonymous_id    VARCHAR(256) ,
context_app_build   VARCHAR(256) ,
context_app_name    VARCHAR(256) ,
context_app_version VARCHAR(256) ,
context_device_ad_tracking_enabled  BOOLEAN ,
context_device_idfa VARCHAR(256) ,
context_device_idfv VARCHAR(256) ,
context_device_manufacturer VARCHAR(256) ,
context_device_model    VARCHAR(256) ,
context_device_type VARCHAR(256) ,
context_ip  VARCHAR(256) ,
context_library_name    VARCHAR(256) ,
context_library_version VARCHAR(256) ,
context_locale  VARCHAR(256) ,
context_network_bluetooth   BOOLEAN ,
context_network_carrier VARCHAR(256) ,
context_network_wifi    BOOLEAN ,
context_os_name VARCHAR(256) ,
context_os_version  VARCHAR(256) ,
context_screen_height   BIGINT ,
context_screen_width    BIGINT ,
event   VARCHAR(256) ,
event_text  VARCHAR(256) ,
original_timestamp  VARCHAR(256) ,
sent_at TIMESTAMP ,
sp_category VARCHAR(256) ,
sp_keyword  VARCHAR(256) ,
timestamp   TIMESTAMP ,
user_id VARCHAR(256) ,
member_id_legacy    BIGINT ,
user_id40   BIGINT ,
uuid_ts TIMESTAMP ,
load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_SEGMENT}/greenpoint_production/incremental/daily/dq_greenpoint_production_search_the_list';
