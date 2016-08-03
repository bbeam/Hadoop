--/*
--  HIVE SCRIPT  : create_dq_member_address.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.MemberAddress) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_member_address.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_member_address
(
	member_id INT,
	member_address_id INT,
	postal_address_id INT,
	adress_type_id INT,
	description STRING,
	market_zone_id INT,
	home_build_year INT,
	create_date TIMESTAMP,
	create_by STRING,
	update_date TIMESTAMP,
	update_by STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_member_address';
