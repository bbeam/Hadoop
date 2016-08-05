--/*
--  HIVE SCRIPT  : create_dq_service_provider_background_check.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.ServiceProviderBackgroundCheck). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_service_provider_background_check.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_service_provider_background_check
(
  service_provider_background_check_id INT,
  service_provider_id INT,
  first_name STRING,
  last_name STRING,
  profile_number STRING,
  completed_date TIMESTAMP,
  background_check_status_id INT,
  create_by STRING,
  create_date TIMESTAMP,
  update_by STRING,
  update_date TIMESTAMP,
  customer_id STRING,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_service_provider_background_check';