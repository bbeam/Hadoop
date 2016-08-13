--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_ad_type.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.ad_type) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_ad_type
(
	ad_type_id INT,
	ad_type_name STRING,
	ad_type_description STRING,
	location_id INT,	
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_ad_type';
