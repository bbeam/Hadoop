--/*
--  HIVE SCRIPT  : create_inc_storefront_order_line_item.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.StorefrontOrderLineItem). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_storefront_order_line_item.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_storefront_order_line_item
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_storefront_order_line_item';