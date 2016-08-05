--/*
--  HIVE SCRIPT  : create_dq_storefront_item_history.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngieHistory.StorefrontItemHistory). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_storefront_item_history.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_item_history
(
  storefront_item_history_id INT,
  storefront_item_id INT,
  storefront_item_status_id INT,
  storefront_item_type_id INT,
  sp_id INT,
  title STRING,
  description STRING,
  redemption_instructions STRING,
  start_datetime TIMESTAMP,
  end_datetime TIMESTAMP,
  contact_name STRING,
  contact_phone STRING,
  contact_email STRING,
  original_price DECIMAL(10,2),
  member_price DECIMAL(10,2),
  storefront_item_fee DECIMAL(5,2),
  ordered_quantity INT,
  payment_approved TINYINT,
  storefront_sales_representative_id INT,
  last_modified_by INT,
  create_date TIMESTAMP,
  create_by STRING,
  update_date TIMESTAMP,
  update_by STRING,
  history_date TIMESTAMP,
  maximum_quantity INT,
  maximum_quantity_per_member INT,
  photo_id INT,
  auto_renew TINYINT,
  paid_quantity INT,
  deal_classification_id INT,
  storefront_item_promo_code_id INT,
  timezone_id INT,
  contract_item_sp_id INT,
  editorial STRING,
  storefront_item_sku STRING,
  history_end_date TIMESTAMP,
  title_with_placeholders STRING,
  description_with_placeholders STRING,
  master_storefront_item_id_for_sku TINYINT,
  member_only TINYINT,
  do_not_override_fulfillment_method TINYINT,
  storefront_order_fulfillment_method_id INT,
  premium_deal TINYINT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angiehistory/full/daily/dq_storefront_item_history';