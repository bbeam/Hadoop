--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_service_provider_group_type.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.ServiceProviderGroupType). 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_service_provider_group_type
(
  service_provider_group_type_id STRING,
  service_provider_group_type_name STRING,
  service_provider_group_type_description STRING,
  service_provider_group_type_status STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_service_provider_group_type';