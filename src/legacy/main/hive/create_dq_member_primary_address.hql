--/*
--  HIVE SCRIPT  : create_dq_member_primary_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.MemberPrimaryAddress) 
--  Execute command:
-- 			hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_member_primary_address.hql \
-- 			-hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- 			-hivevar S3_BUCKET=$S3_BUCKET \ 
-- 			-hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_member_primary_address
(
	member_primary_address_id INT ,
	member_id INT ,
	member_address_id INT ,
	known_invalid_postal_address TIMESTAMP ,
	create_date TIMESTAMP ,
	create_by STRING,
	update_date TIMESTAMP ,
	update_by STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_member_primary_address';
