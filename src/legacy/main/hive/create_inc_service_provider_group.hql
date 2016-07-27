--/*
--  HIVE SCRIPT  : create_inc_service_provider_group.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProviderGroup) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.service_provider_group
(
	service_provider_group STRING, 
	service_provider_group_description STRING, 
	service_provider_group_id STRING, 
	service_provider_group_photo_id STRING, 
	service_provider_group_type_id STRING
)
PARTITIONED BY (bus_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/service_provider_group';
