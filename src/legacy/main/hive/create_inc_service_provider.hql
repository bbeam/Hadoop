--/*
--  HIVE SCRIPT  : create_inc_service_provider.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 01, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProvider). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_service_provider.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_service_provider
(
  sp_id STRING,
  sp_status_id STRING,
  company_name STRING,
  entered_date STRING,
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
  emergency_service STRING,
  create_date STRING,
  create_by STRING,
  country_code_id STRING,
  legal_business_name STRING,
  classic_import_work_id STRING,
  known_invalid_email_address STRING,
  postal_address_status_id STRING,
  phone_status_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_service_provider';