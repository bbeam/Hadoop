--  HIVE SCRIPT  : create_dq_angieslistweb_prod_purchased_cart.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(dq_purchased_cart). 
--*/

--  Creating a DQ hive table(dq_purchased_cart) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_purchased_cart
( 
id  VARCHAR(254),
est_received_at TIMESTAMP,
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
est_original_timestamp  TIMESTAMP,
original_timestamp  TIMESTAMP,
purchased_items VARCHAR(256),
est_sent_at TIMESTAMP,
sent_at TIMESTAMP,
est_timestamp   TIMESTAMP,
timestamp   TIMESTAMP,
total_al_take   VARCHAR(256),
total_revenue   VARCHAR(256),
user_id VARCHAR(256),
user_zip_code   VARCHAR(256),
est_uuid_ts TIMESTAMP,
uuid_ts TIMESTAMP,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/segment/events/angieslistweb_prod/incremental/daily/dq_purchased_cart';

