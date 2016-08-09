--/*
--  HIVE SCRIPT  : create_dq_service_provider_group.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.ServiceProviderGroup) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_service_provider_group
(
	service_provider_group_id INT, 
	service_provider_group STRING, 
	service_provider_group_description STRING, 
	service_provider_group_type_id INT,
	service_provider_group_photo_id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp	TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_service_provider_group';
