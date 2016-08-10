--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_storefront_order.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.StorefrontOrder). 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_storefront_order
(
  storefront_order_id STRING,
  storefront_order_status_id STRING,
  storefront_source_id STRING,
  member_id STRING,
  payment_profile_id STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING,
  storefront_order_fulfillment_method_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/incremental/daily/inc_storefront_order';