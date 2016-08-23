--/*
--  HIVE SCRIPT  : create_gold_dim_members.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.dim_members 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_members
(
	member_key BIGINT,
	member_id INT,
	user_id INT,
	email STRING,
	postal_code STRING,
	pay_status STRING,
	member_status STRING,
	expiration_status STRING,
	member_dt TIMESTAMP,
	membership_tier_nm STRING,
	primary_phone_number STRING,
	first_nm STRING,
	last_nm STRING,
	associate TINYINT,
	employee TINYINT,
	market_key BIGINT
)
PARTITIONED BY (edh_bus_month STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_members';