--/*
--  HIVE SCRIPT  : create_dq_storefront_item.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_item). 
--  USAGE        :
-- hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_storefront_item.hql \
-- -hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
-- -hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
-- -hivevar S3_BUCKET="${S3_BUCKET}" 
--*/

--  Creating a dq hive table(dq_storefront_item) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_item
(
  storefront_item_id INT,
  storefront_item_status_id INT,
  storefront_item_type_id INT,
  sp_id INT,
  title STRING,
  description STRING,
  redemption_instructions STRING,
  start_date_time TIMESTAMP,
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
  create_date TIMESTAMP,
  create_by STRING,
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
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_storefront_item';
