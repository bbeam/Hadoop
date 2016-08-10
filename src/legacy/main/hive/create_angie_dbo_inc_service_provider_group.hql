--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_service_provider_group.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProviderGroup) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_service_provider_group
(
	service_provider_group_id STRING, 
	service_provider_group STRING, 
	service_provider_group_description STRING, 
	service_provider_group_type_id STRING,
	service_provider_group_photo_id STRING	
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_service_provider_group';
