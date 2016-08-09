--/*
--  HIVE SCRIPT  : create_inc_ad_type.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.AdType) 

--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_ad_type
(
	ad_type_id STRING,
	ad_type_name STRING,
	ad_type_description STRING,
	location_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_ad_type';
