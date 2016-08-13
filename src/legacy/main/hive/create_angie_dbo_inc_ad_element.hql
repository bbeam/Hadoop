--/*
--  HIVE SCRIPT  : create_inc_ad_element.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.AdElement).
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_ad_element
(
  ad_element_id STRING,
  ad_element_name STRING,
  ad_element_description STRING,
  ad_element_active STRING,
  ad_element_inventory_model_id STRING,
  default_price STRING,
  currency_code_id STRING,
  ad_type_id STRING,
  service_provider_pricing_model_id STRING,
  can_sell_at_market_level STRING,
  min_paid_months STRING,
  max_paid_months STRING,
  ad_element_color STRING,
  billing_lead_time STRING,
  is_advertiser STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_ad_element';