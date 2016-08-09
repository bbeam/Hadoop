--/*
--  HIVE SCRIPT  : create_dq_ad_type.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.ad_type) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_ad_type.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_ad_type
(
	ad_type_id INT,
	ad_type_name STRING,
	ad_type_description STRING,
	location_id INT,	
	load_timestamp	TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_ad_type';
