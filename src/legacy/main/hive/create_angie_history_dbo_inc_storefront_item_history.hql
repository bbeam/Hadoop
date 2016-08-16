--/*
--  HIVE SCRIPT  : create_angie_history_dbo_inc_storefront_item_history.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngieHistory.StorefrontItemHistory). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_storefront_item_history
(
  storefront_item_history_id STRING,
  storefront_item_id STRING,
  storefront_item_status_id STRING,
  storefront_item_type_id STRING,
  sp_id STRING,
  title STRING,
  description STRING,
  redemption_instructions STRING,
  start_datetime STRING,
  end_datetime STRING,
  contact_name STRING,
  contact_phone STRING,
  contact_email STRING,
  original_price STRING,
  member_price STRING,
  storefront_item_fee STRING,
  ordered_quantity STRING,
  payment_approved STRING,
  storefront_sales_representative_id STRING,
  last_modified_by STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING,
  history_date STRING,
  maximum_quantity STRING,
  maximum_quantity_per_member STRING,
  photo_id STRING,
  auto_renew STRING,
  paid_quantity STRING,
  deal_classification_id STRING,
  storefront_item_promo_code_id STRING,
  timezone_id STRING,
  contract_item_sp_id STRING,
  editorial STRING,
  storefront_item_sku STRING,
  history_end_date STRING,
  title_with_placeholders STRING,
  description_with_placeholders STRING,
  master_storefront_item_id_for_sku STRING,
  member_only STRING,
  do_not_override_fulfillment_method STRING,
  storefront_order_fulfillment_method_id STRING,
  premium_deal STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angiehistory/dbo/incremental/daily/inc_storefront_item_history';