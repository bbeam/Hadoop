--/*
--  HIVE SCRIPT  : create_inc_member_primary_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProvider). 
--  Execute command:
-- 				hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_member_primary_address.hql \
-- 				-hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \ 
-- 				-hivevar S3_BUCKET=$S3_BUCKET \ 
-- 				-hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_member_primary_address
(
	member_primary_address_id STRING ,
	member_id STRING ,
	member_address_id STRING ,
	known_invalid_postal_address STRING ,
	create_date STRING ,
	create_by STRING,
	update_date STRING ,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_member_primary_address';