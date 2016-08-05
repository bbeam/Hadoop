--/*
--  HIVE SCRIPT  : create_dq_contract.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.Contract). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_contract.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_contract
(
  contract_id INT,
  title STRING,
  contract_workflow_status_id INT,
  contract_type_id INT,
  contract_template_id INT,
  display_pricing TINYINT,
  employee_id INT,
  display_group_pricing TINYINT,
  country_code_id INT,
  currency_code_id INT,
  invoice_generated TINYINT,
  sp_id INT,
  first_payment_received_status_id INT,
  first_payment_profile_id INT,
  payment_profile_id INT,
  contract_payment_method_id INT,
  first_payment_type_id INT,
  payment_type_id INT,
  payment_frequency_id INT,
  contract_delivery_method_id INT,
  service_provider_contact_id INT,
  sp_membership_id INT,
  ad_discount_schedule_id INT,
  anniversary_day INT,
  published_date TIMESTAMP,
  signed_date TIMESTAMP,
  oca_expiration_date TIMESTAMP,
  oca_associate_message STRING,
  ccv_date TIMESTAMP,
  notes_id INT,
  modified_by_employee_id INT,
  create_date TIMESTAMP,
  create_by STRING,
  modified_date TIMESTAMP,
  activation_date TIMESTAMP,
  ad_sale_employee_id INT,
  is_amended TINYINT,
  publishing_department_id INT,
  attrition_account_manager_id INT,
  contract_version_number INT,
  sales_force_quote_id STRING,
  sales_force_opportunity_id STRING,
  national_indicator TINYINT,
  show_in_business_center TINYINT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_contract';