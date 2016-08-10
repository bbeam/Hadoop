--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_storefront_order.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.StorefrontOrder). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_order
(
  storefront_order_id INT,
  storefront_order_status_id INT,
  storefront_source_id INT,
  member_id INT,
  payment_profile_id INT,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by STRING,
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  update_by STRING,
  storefront_order_fulfillment_method_id INT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/incremental/daily/dq_storefront_order';
