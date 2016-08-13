--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_service_provider_background_check.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.ServiceProviderBackgroundCheck). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_service_provider_background_check
(
  service_provider_background_check_id INT,
  service_provider_id INT,
  first_name STRING,
  last_name STRING,
  profile_number STRING,
  est_completed_date TIMESTAMP,
  completed_date TIMESTAMP,
  background_check_status_id INT,
  create_by STRING,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  update_by STRING,
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  customer_id STRING,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_service_provider_background_check';
