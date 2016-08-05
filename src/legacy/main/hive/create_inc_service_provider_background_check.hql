--/*
--  HIVE SCRIPT  : create_inc_service_provider_background_check.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.ServiceProviderBackgroundCheck). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_service_provider_background_check.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_service_provider_background_check
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_service_provider_background_check';