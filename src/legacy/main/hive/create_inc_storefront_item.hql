--/*
--  HIVE SCRIPT  : create_inc_storefront_item.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_storefront_item). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_storefront_item.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_storefront_item) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_storefront_item
(
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
  maximum_quantity STRING,
  maximum_quantity_per_member STRING,
  photo_id STRING,
  auto_renew STRING,
  contract_item_sp_id STRING,
  paid_quantity STRING,
  storefront_item_promo_code_id STRING,
  time_zone_id STRING,
  editorial STRING,
  storefront_item_sku STRING,
  title_with_placeholders STRING,
  description_with_placeholders STRING,
  master_storefront_item_id_for_sku STRING,
  member_only STRING,
  employee_owner_id STRING,
  storefront_order_fulfillment_method_id STRING,
  do_not_override_fulfillment_method STRING,
  premium_deal STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_storefront_item';
