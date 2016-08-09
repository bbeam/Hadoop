--/*
--  HIVE SCRIPT  : create_dq_member_primary_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.MemberPrimaryAddress) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_member_primary_address
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_member_primary_address';
