--/*
--  HIVE SCRIPT  : create_inc_angieslistweb_prod_user_login.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_angieslistweb_prod_user_login). 
--*/
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_user_login
(
id STRING,
received_at STRING,
uuid STRING,
context_auth_token STRING,
context_ip STRING,
context_library_name STRING,
context_library_version STRING,
context_referer STRING,
context_user_agent STRING,
event STRING,
event_text STRING,
original_timestamp STRING,
sent_at STRING,
source STRING,
timestamp STRING,
user_id STRING,
user_zip_code STRING,
anonymous_id STRING,
context_page_path STRING,
context_page_title STRING,
context_page_url STRING,
uuid_ts STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/segment/events/angieslistweb_prod/incremental/daily/inc_user_login';