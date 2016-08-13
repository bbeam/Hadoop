--/*
--  HIVE SCRIPT  : create_dq_greenpoint_android_prod_review_submitted.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(dq_review_submitted). 
--*/

--  Creating a DQ hive table(dq_review_submitted) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_review_submitted
( 
id VARCHAR(254),
est_received_at TIMESTAMP,
received_at TIMESTAMP,
uuid BIGINT,
anonymous_id VARCHAR(256),
context_app_build BIGINT,
context_app_name VARCHAR(256),
context_app_namespace VARCHAR(256),
context_app_version VARCHAR(256),
context_device_ad_tracking_enabled BOOLEAN,
context_device_advertising_id VARCHAR(256),
context_device_id VARCHAR(256),
context_device_manufacturer VARCHAR(256),
context_device_model VARCHAR(256),
context_device_name VARCHAR(256),
context_device_type VARCHAR(256),
context_ip VARCHAR(256),
context_library_name VARCHAR(256),
context_library_version VARCHAR(256),
context_library_version_name VARCHAR(256),
context_locale VARCHAR(256),
context_network_bluetooth BOOLEAN,
context_network_carrier VARCHAR(256),
context_network_cellular BOOLEAN,
context_network_wifi BOOLEAN,
context_os_name VARCHAR(256),
context_os_sdk BIGINT,
context_os_version VARCHAR(256),
context_screen_density BIGINT,
context_screen_density_bucket VARCHAR(256),
context_screen_density_dpi BIGINT,
context_screen_height BIGINT,
context_screen_scaled_density BIGINT,
context_screen_width BIGINT,
context_timezone VARCHAR(256),
context_traits_anonymous_id VARCHAR(256),
context_traits_email VARCHAR(256),
context_traits_user_id VARCHAR(256),
context_user_agent VARCHAR(256),
event VARCHAR(256),
event_text VARCHAR(256),
member_id BIGINT,
original_timestamp VARCHAR(256),
est_sent_at TIMESTAMP,
sent_at TIMESTAMP,
service_provider_id BIGINT,
est_timestamp TIMESTAMP,
timestamp TIMESTAMP,
user_id VARCHAR(256),
member_id_legacy BIGINT,
user_id40 BIGINT,
est_uuid_ts TIMESTAMP,
uuid_ts TIMESTAMP,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/segment/events/greenpoint_android_prod/incremental/daily/dq_review_submitted';