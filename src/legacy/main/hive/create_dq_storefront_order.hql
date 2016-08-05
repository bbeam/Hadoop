--/*
--  HIVE SCRIPT  : create_dq_storefront_order.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.StorefrontOrder). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_storefront_order.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_order
(
  storefront_order_id INT,
  storefront_order_status_id INT,
  storefront_source_id INT,
  member_id INT,
  payment_profile_id INT,
  create_date TIMESTAMP,
  create_by STRING,
  update_date TIMESTAMP,
  update_by STRING,
  storefront_order_fulfillment_method_id INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/incremental/daily/dq_storefront_order';