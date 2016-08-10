--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_contract_item.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.ContractItem). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_contract_item
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
  est_start_date TIMESTAMP,
  start_date TIMESTAMP,
  est_end_date TIMESTAMP,
  end_date TIMESTAMP,
  classified_ad_text STRING,
  coupon_text STRING,
  ad_note STRING,
  paid_months INT,
  premium DECIMAL(10,2),
  list_price DECIMAL(10,2),
  pre_discounted_total DECIMAL(10,2),
  total DECIMAL(10,2),
  monthly_price DECIMAL(10,2),
  color TINYINT,
  product_description STRING,
  product_type_other STRING,
  billing_lead_time INT,
  employee_id INT,
  modified_by_employee_id INT,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by STRING,
  est_modified_date TIMESTAMP,
  modified_date TIMESTAMP,
  est_targeted_end_date TIMESTAMP,
  targeted_end_date TIMESTAMP,
  est_targeted_end_date_changed_date TIMESTAMP,
  targeted_end_date_changed_date TIMESTAMP,
  targeted_end_date_changed_by_employee_id INT,
  revenue_change_reason_id INT,
  eligibility_date DATE,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_contract_item';
