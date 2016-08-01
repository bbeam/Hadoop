--/*
--  HIVE SCRIPT  : create_dq_contract_item.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.ContractItem). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_contract_item.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.dq_contract_item
(
  contract_item_id INT,
  ad_element_id INT,
  contract_id INT,
  use_seasonal_schedule TINYINT,
  is_national TINYINT,
  contract_item_status_id INT,
  market_id INT,
  country_code_id INT,
  currency_code_id INT,
  contract_item_pulled_reason_id INT,
  category_id INT,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  classified_ad_text STRING,
  coupon_text STRING,
  ad_note STRING,
  paid_months INT,
  premium DECIMAL,
  list_price DECIMAL,
  pre_discounted_total DECIMAL,
  total DECIMAL,
  monthly_price DECIMAL,
  color TINYINT,
  product_description STRING,
  product_type_other STRING,
  billing_lead_time INT,
  employee_id INT,
  modified_by_employee_id INT,
  create_date TIMESTAMP,
  create_by STRING,
  modified_date TIMESTAMP,
  targeted_end_date TIMESTAMP,
  targeted_end_date_changed_date TIMESTAMP,
  targeted_end_date_changed_by_employee_id INT,
  revenue_change_reason_id INT,
  eligibility_date TIMESTAMP,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/${hivevar:S3_LOCATION_INCOMING_DATA}/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_contract_item';