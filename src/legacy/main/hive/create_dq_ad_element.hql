--/*
--  HIVE SCRIPT  : create_dq_ad_element.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.AdElement). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_ad_element.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_ad_element
(
  ad_element_id INT,
  ad_element_name STRING,
  ad_element_description STRING,
  ad_element_active TINYINT,
  ad_element_inventory_model_id INT,
  default_price decimal(10,2),
  currency_code_id INT,
  ad_type_id INT,
  service_provider_pricing_model_id INT,
  can_sell_at_market_level TINYINT,
  min_paid_months INT,
  max_paid_months INT,
  ad_element_color TINYINT,
  billing_lead_time INT,
  is_advertiser TINYINT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_ad_element';