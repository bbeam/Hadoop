--/*
--  HIVE SCRIPT  : create_dq_greenpoint_production_screens.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(dq_screens). 
--*/

--  Creating a DQ hive table(dq_screens) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_screens
( 
id VARCHAR(254),
est_received_at TIMESTAMP,
received_at TIMESTAMP,
uuid BIGINT,
anonymous_id VARCHAR(256),
app_version VARCHAR(256),
category VARCHAR(256),
context_app_build VARCHAR(256),
context_app_name VARCHAR(256),
context_app_version VARCHAR(256),
context_device_ad_tracking_enabled BOOLEAN,
context_device_idfa VARCHAR(256),
context_device_idfv VARCHAR(256),
context_device_manufacturer VARCHAR(256),
context_device_model VARCHAR(256),
context_device_type VARCHAR(256),
context_ip VARCHAR(256),
context_library_name VARCHAR(256),
context_library_version VARCHAR(256),
context_locale VARCHAR(256),
context_network_bluetooth BOOLEAN,
context_network_carrier VARCHAR(256),
context_network_wifi BOOLEAN,
context_os_name VARCHAR(256),
context_os_version VARCHAR(256),
context_screen_height BIGINT,
context_screen_width BIGINT,
count BIGINT,
coupon VARCHAR(256),
date VARCHAR(256),
deal_id VARCHAR(256),
is_logged_in VARCHAR(256),
keyword VARCHAR(256),
market_id VARCHAR(256),
member_id VARCHAR(256),
name VARCHAR(256),
offer_item_id VARCHAR(256),
offer_item_name VARCHAR(256),
op_system VARCHAR(256),
original_timestamp VARCHAR(256),
phone_model VARCHAR(256),
reason VARCHAR(256),
est_sent_at TIMESTAMP,
sent_at TIMESTAMP,
sort_by VARCHAR(256),
sp_id BIGINT,
specific_screen VARCHAR(256),
spid BIGINT,
spidview BIGINT,
est_timestamp TIMESTAMP,
timestamp TIMESTAMP,
user_id VARCHAR(256),
zipcode VARCHAR(256),
user_id40 BIGINT,
member_id_legacy BIGINT,
est_uuid_ts TIMESTAMP,
uuid_ts TIMESTAMP,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/segment/events/greenpoint_production/incremental/daily/dq_screens';

