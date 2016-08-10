--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_member_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.MemberAddress) 
--  Execute command:
-- 	hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_angie_dbo_inc_member_address.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_member_address
(
	member_id STRING,
	member_address_id STRING,
	postal_address_id STRING,
	adress_type_id STRING,
	description STRING,
	market_zone_id STRING,
	home_build_year STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_member_address';
