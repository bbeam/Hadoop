--  HIVE SCRIPT  : create_dq_greenpoint_android_prod_popular_category_tapped.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(Dq_greenpoint_android_prod_popular_category_tapped). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_dq_greenpoint_android_prod_popular_category_tapped.hql \
--     --hivevar SEGMENT_GOLD_DB="${SEGMENT_GOLD_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 

--*/

--  Creating a DQ hive table(Dq_greenpoint_android_prod_popular_category_tapped) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_GOLD_DB}.dq_greenpoint_android_prod_popular_category_tapped
( 
id  VARCHAR(254) ,
received_at TIMESTAMP ,
uuid    BIGINT ,
anonymous_id    VARCHAR(256) ,
context_app_build   BIGINT ,
context_app_name    VARCHAR(256) ,
context_app_namespace   VARCHAR(256) ,
context_app_version VARCHAR(256) ,
context_device_ad_tracking_enabled  boolean ,
context_device_advertising_id   VARCHAR(256) ,
context_device_id   VARCHAR(256) ,
context_device_manufacturer VARCHAR(256) ,
context_device_model    VARCHAR(256) ,
context_device_name VARCHAR(256) ,
context_device_type VARCHAR(256) ,
context_ip  VARCHAR(256) ,
context_library_name    VARCHAR(256) ,
context_library_version VARCHAR(256) ,
context_library_version_name    VARCHAR(256) ,
context_locale  VARCHAR(256) ,
context_network_bluetooth   boolean ,
context_network_carrier VARCHAR(256) ,
context_network_cellular    boolean ,
context_network_wifi    boolean ,
context_os_name VARCHAR(256) ,
context_os_sdk  BIGINT ,
context_os_version  VARCHAR(256) ,
context_screen_density  DOUBLE ,
context_screen_density_bucket   VARCHAR(256) ,
context_screen_density_dpi  BIGINT ,
context_screen_height   BIGINT ,
context_screen_scaled_density   DOUBLE ,
context_screen_width    BIGINT ,
context_timezone    VARCHAR(256) ,
context_traits_anonymous_id VARCHAR(256) ,
context_traits_email    VARCHAR(256) ,
context_traits_user_id  VARCHAR(256) ,
context_user_agent  VARCHAR(256) ,
event   VARCHAR(256) ,
event_text  VARCHAR(256) ,
original_timestamp  VARCHAR(256) ,
popular_category_name   VARCHAR(256) ,
sent_at TIMESTAMP ,
timestamp   TIMESTAMP ,
user_id VARCHAR(256) ,
user_id40   BIGINT ,
member_id_legacy    BIGINT ,
uuid_ts TIMESTAMP ,
load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_SEGMENT}/greenpoint_android_prod/incremental/daily/dq_greenpoint_android_prod_popular_category_tapped';
