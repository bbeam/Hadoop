--/*
--  HIVE SCRIPT  : create_dq_service_provider.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.ServiceProvider).
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_service_provider
(
  sp_id INT,
  sp_status_id INT,
  company_name STRING,
  est_entered_date TIMESTAMP,
  entered_date TIMESTAMP,
  address_1 STRING,
  address_2 STRING,
  city STRING,
  state STRING,
  zip STRING,
  business_date STRING,
  contact_title STRING,
  contact_first STRING,
  contact_middle STRING,
  contact_last STRING,
  phone STRING,
  phone_2 STRING,
  voice_star_number STRING,
  fax STRING,
  email STRING,
  url STRING,
  senior_discount STRING,
  non_services STRING,
  services STRING,
  operating_hours STRING,
  service_area STRING,
  bus_desc STRING,
  bbb STRING,
  exclude_reason STRING,
  internal_comment STRING,
  emergency_service TINYINT,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by STRING,
  country_code_id INT,
  legal_business_name STRING,
  classic_import_work_id INT,
  known_invalid_email_address TINYINT,
  postal_address_status_id INT,
  phone_status_id INT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_service_provider';
