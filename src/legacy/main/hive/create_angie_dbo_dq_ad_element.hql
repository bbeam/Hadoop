--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_ad_element.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.AdElement).
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_ad_element
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
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_ad_element';
