--/*
--  HIVE SCRIPT  : create_dq_storefront_order_line_item.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.StorefrontOrderLineItem). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_order_line_item
(
  storefront_order_line_item_id INT,
  storefront_order_id INT,
  storefront_order_line_item_status_id INT,
  storefront_order_line_item_quantity INT,
  storefront_item_id INT,
  cash_posting_id INT,
  storefront_order_line_item_fee DECIMAL(9, 2),
  member_sp_communication_id INT,
  storefront_item_fee DECIMAL(10, 2),
  storefront_source_id INT,
  marked_read TINYINT,
  est_ load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_storefront_order_line_item';
