--/*
--  HIVE SCRIPT  : create_inc_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.PostalAddress). 
--  Execute command:
-- 				hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_postal_address.hql \
-- 				-hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \ 
-- 				-hivevar S3_BUCKET=$S3_BUCKET \ 
-- 				-hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_postal_address
(
	postal_address_id STRING,
	postal_format_id STRING,
	address1 STRING,
	address2 STRING,
	city STRING,
	state STRING,
	postal_code STRING,
	country_code_id STRING,
	latitude STRING,
	longitude STRING,
	coordinate_confidence STRING,
	process_date STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_postal_address';