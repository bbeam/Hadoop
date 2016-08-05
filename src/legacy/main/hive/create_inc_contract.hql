--/*
--  HIVE SCRIPT  : create_inc_contract.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.Contract). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_contract.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_contract
(
  contract_id STRING,
  title STRING,
  contract_workflow_status_id STRING,
  contract_type_id STRING,
  contract_template_id STRING,
  display_pricing STRING,
  employee_id STRING,
  display_group_pricing STRING,
  country_code_id STRING,
  currency_code_id STRING,
  invoice_generated STRING,
  sp_id STRING,
  first_payment_received_status_id STRING,
  first_payment_profile_id STRING,
  payment_profile_id STRING,
  contract_payment_method_id STRING,
  first_payment_type_id STRING,
  payment_type_id STRING,
  payment_frequency_id STRING,
  contract_delivery_method_id STRING,
  service_provider_contact_id STRING,
  sp_membership_id STRING,
  ad_discount_schedule_id STRING,
  anniversary_day STRING,
  published_date STRING,
  signed_date STRING,
  oca_expiration_date STRING,
  oca_associate_message STRING,
  ccv_date STRING,
  notes_id STRING,
  modified_by_employee_id STRING,
  create_date STRING,
  create_by STRING,
  modified_date STRING,
  activation_date STRING,
  ad_sale_employee_id STRING,
  is_amended STRING,
  publishing_department_id STRING,
  attrition_account_manager_id STRING,
  contract_version_number STRING,
  sales_force_quote_id STRING,
  sales_force_opportunity_id STRING,
  national_indicator STRING,
  show_in_business_center STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_contract';