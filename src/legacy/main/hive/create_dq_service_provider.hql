--/*
--  HIVE SCRIPT  : create_dq_service_provider.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.ServiceProvider). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_service_provider.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_service_provider
(
  sp_id INT,
  sp_status_id INT,
  company_name STRING,
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
  create_date TIMESTAMP,
  create_by STRING,
  country_code_id INT,
  legal_business_name STRING,
  classic_import_work_id INT,
  known_invalid_email_address TINYINT,
  postal_address_status_id INT,
  phone_status_id INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_service_provider';