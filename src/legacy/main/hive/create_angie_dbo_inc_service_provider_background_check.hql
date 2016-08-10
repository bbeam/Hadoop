--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_service_provider_background_check.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.ServiceProviderBackgroundCheck). 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_service_provider_background_check
(
  service_provider_background_check_id STRING,
  service_provider_id STRING,
  first_name STRING,
  last_name STRING,
  profile_number STRING,
  completed_date STRING,
  background_check_status_id STRING,
  create_by STRING,
  create_date STRING,
  update_by STRING,
  update_date STRING,
  customer_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_service_provider_background_check';