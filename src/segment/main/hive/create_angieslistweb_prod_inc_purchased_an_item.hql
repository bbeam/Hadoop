--/*
--  HIVE SCRIPT  : create_angieslistweb_prod_inc_purchased_an_item.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_purchased_an_item). 
--*/

--  Creating a incoming hive table(inc_purchased_an_item) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_purchased_an_item
(
id STRING,
received_at STRING,
uuid STRING,
cart_id STRING,
cart_value STRING,
context_auth_token STRING,
context_ip STRING,
context_library_name STRING,
context_library_version STRING,
context_referer STRING,
context_user_agent STRING,
event STRING,
event_text STRING,
invoice_id STRING,
invoice_item_id STRING,
item_al_take STRING,
item_revenue STRING,
job_offer_id STRING,
original_STRING STRING,
sent_at STRING,
sku_category STRING,
sku_category_name STRING,
sku_id STRING,
sku_item STRING,
sku_template_id STRING,
sku_title STRING,
sp_name STRING,
spid STRING,
timestamp STRING,
user_id STRING,
user_zip_code STRING,
uuid_ts STRING
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/segment/events/angieslistweb_prod/incremental/daily/inc_purchased';