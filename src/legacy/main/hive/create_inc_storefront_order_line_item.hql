--/*
--  HIVE SCRIPT  : create_inc_storefront_order_line_item.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.StorefrontOrderLineItem). 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_storefront_order_line_item
(
  storefront_order_line_item_id STRING,
  storefront_order_id STRING,
  storefront_order_line_item_status_id STRING,
  storefront_order_line_item_quantity STRING,
  storefront_item_id STRING,
  cash_posting_id STRING,
  storefront_order_line_item_fee STRING,
  member_sp_communication_id STRING,
  storefront_item_fee STRING,
  storefront_source_id STRING,
  marked_read STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_storefront_order_line_item';