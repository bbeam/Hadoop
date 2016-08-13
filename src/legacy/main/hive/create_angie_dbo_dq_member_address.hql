--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_member_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.MemberAddress) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_member_address
(
	member_id INT,
	member_address_id INT,
	postal_address_id INT,
	adress_type_id INT,
	description STRING,
	market_zone_id INT,
	home_build_year INT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_member_address';
