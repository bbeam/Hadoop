--/*
--  HIVE SCRIPT  : create_inc_angieslistweb_prod_user_login_successful.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_angieslistweb_prod_user_login_successful). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_inc_angieslistweb_prod_user_login_successful.hql \
     --hivevar SEGMENT_INCOMING_DB="${SEGMENT_INCOMING_DB}" \
     --hivevar S3_BUCKET="${S3_BUCKET}" \
     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 
--*/

--  Creating a incoming hive table(inc_angieslistweb_prod_user_login_successful) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_INCOMING_DB}.inc_angieslistweb_prod_user_login_successful
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
timestamp STRING,
user_id STRING,
user_zip_code STRING,
uuid_ts STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_SEGMENT}/angieslistweb_prod/incremental/daily/inc_angieslistweb_prod_user_login_successful';