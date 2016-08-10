--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_storefront_item.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_item). 
--*/

--  Creating a dq hive table(dq_storefront_item) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_item
(
  storefront_item_id INT,
  storefront_item_status_id INT,
  storefront_item_type_id INT,
  sp_id INT,
  title STRING,
  description STRING,
  redemption_instructions STRING,
  est_start_date_time TIMESTAMP,
  start_date_time TIMESTAMP,
  est_end_date_time TIMESTAMP,
  end_date_time TIMESTAMP,
  contact_name STRING,
  contact_phone STRING,
  contact_email STRING,
  original_price DECIMAL(10,2),
  member_price DECIMAL(10,2),
  storefront_item_fee DECIMAL(10,2),
  ordered_quantity INT,
  payment_approved TINYINT,
  storefront_sales_representative_id INT,
  last_modified_by INT,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by STRING,
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  update_by STRING,
  maximum_quantity INT,
  maximum_quantity_per_member INT,
  photo_id INT,
  auto_renew TINYINT,
  contract_item_sp_id INT,
  paid_quantity INT,
  storefront_item_promo_code_id INT,
  time_zone_id INT,
  editorial STRING,
  storefront_item_sku STRING,
  title_with_placeholders STRING,
  description_with_placeholders STRING,
  master_storefront_item_id_for_sku TINYINT,
  member_only TINYINT,
  employee_owner_id INT,
  storefront_order_fulfillment_method_id INT,
  do_not_override_fulfillment_method TINYINT,
  premium_deal TINYINT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_storefront_item';
