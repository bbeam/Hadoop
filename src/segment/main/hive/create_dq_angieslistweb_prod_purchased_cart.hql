--  HIVE SCRIPT  : create_dq_angieslistweb_prod_purchased_cart.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(Dq_angieslistweb_prod_purchased_cart). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_dq_angieslistweb_prod_purchased_cart.hql \
--     --hivevar SEGMENT_GOLD_DB="${SEGMENT_GOLD_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 

--*/

--  Creating a DQ hive table(Dq_angieslistweb_prod_purchased_cart) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_GOLD_DB}.dq_angieslistweb_prod_purchased_cart
( 
id  VARCHAR(254),
received_at TIMESTAMP,
uuid    BIGINT,
cart_id VARCHAR(256),
context_auth_token  VARCHAR(256),
context_ip  VARCHAR(256),
context_library_name    VARCHAR(256),
context_library_version VARCHAR(256),
context_referer VARCHAR(256),
context_user_agent  VARCHAR(256),
event   VARCHAR(256),
event_text  VARCHAR(256),
original_timestamp  TIMESTAMP,
purchased_items VARCHAR(256),
sent_at TIMESTAMP,
timestamp   TIMESTAMP,
total_al_take   VARCHAR(256),
total_revenue   VARCHAR(256),
user_id VARCHAR(256),
user_zip_code   VARCHAR(256),
uuid_ts TIMESTAMP,
load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_SEGMENT}/angieslistweb_prod/incremental/daily/dq_angieslistweb_prod_purchased_cart';

