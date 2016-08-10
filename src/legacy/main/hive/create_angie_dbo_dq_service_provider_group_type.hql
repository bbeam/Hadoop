--/*
--  HIVE SCRIPT  : create_dq_service_provider_group_type.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.ServiceProviderGroupType). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_service_provider_group_type
(
  service_provider_group_type_id INT,
  service_provider_group_type_name STRING,
  service_provider_group_type_description STRING,
  service_provider_group_type_status TINYINT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_service_provider_group_type';
