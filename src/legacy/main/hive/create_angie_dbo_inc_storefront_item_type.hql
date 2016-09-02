--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_storefront_item_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_storefront_item_type). 
--*/

--  Creating a incoming hive table(legacy_storefront_item_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_storefront_item_type
(
  storefront_item_type_id STRING,
  storefront_item_type_name STRING,
  web_content_key STRING,
  receipt_email_object STRING,
  instructions_email_object STRING,
  service_provider_email_object STRING,
  create_date STRING,
  create_by STRING,
  storefront_item_type_default_fee STRING,
  product_id STRING,
  checksum STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_storefront_item_type';