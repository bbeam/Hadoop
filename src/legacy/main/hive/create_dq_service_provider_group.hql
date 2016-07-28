--/*
--  HIVE SCRIPT  : create_dq_service_provider_group.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.ServiceProviderGroup) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_service_provider_group.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_service_provider_group
(
	service_provider_group STRING, 
	service_provider_group_description STRING, 
	service_provider_group_id INT, 
	service_provider_group_photo_id INT,
	service_provider_group_type_id INT,
	load_timestamp	TIMESTAMP
)
STORED AS ORC
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_service_provider_group';
