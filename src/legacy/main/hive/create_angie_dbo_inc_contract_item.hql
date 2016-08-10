--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_contract_item.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ContractItem). 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_contract_item
(
  contract_item_id STRING,
  ad_element_id STRING,
  contract_id STRING,
  use_seasonal_schedule STRING,
  is_national STRING,
  contract_item_status_id STRING,
  market_id STRING,
  country_code_id STRING,
  currency_code_id STRING,
  contract_item_pulled_reason_id STRING,
  category_id STRING,
  start_date STRING,
  end_date STRING,
  classified_ad_text STRING,
  coupon_text STRING,
  ad_note STRING,
  paid_months STRING,
  premium STRING,
  list_price STRING,
  pre_discounted_total STRING,
  total STRING,
  monthly_price STRING,
  color STRING,
  product_description STRING,
  product_type_other STRING,
  billing_lead_time STRING,
  employee_id STRING,
  modified_by_employee_id STRING,
  create_date STRING,
  create_by STRING,
  modified_date STRING,
  targeted_end_date STRING,
  targeted_end_date_changed_date STRING,
  targeted_end_date_changed_by_employee_id STRING,
  revenue_change_reason_id STRING,
  eligibility_date STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_contract_item';