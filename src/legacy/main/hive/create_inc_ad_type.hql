--/*
--  HIVE SCRIPT  : create_inc_ad_type.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.AdType) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_ad_type.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_ad_type
(
	ad_type_id STRING,
	ad_type_name STRING,
	ad_type_description STRING,
	location_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_ad_type';
