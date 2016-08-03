--/*
--  HIVE SCRIPT  : create_dq_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.PostalAddress) 
--  Execute command:
-- 			hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_postal_address.hql \
-- 			-hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- 			-hivevar S3_BUCKET=$S3_BUCKET \ 
-- 			-hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_postal_address
(
	postal_address_id INT,
	postal_format_id INT,
	address1 STRING,
	address2 STRING,
	city STRING,
	state STRING,
	postal_code STRING,
	country_code_id INT,
	latitude DECIMAL(19,9),
	longitude DECIMAL(19,9),
	coordinate_confidence INT,
	process_date TIMESTAMP,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_postal_address';
