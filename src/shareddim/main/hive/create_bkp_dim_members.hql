--/*
--  HIVE SCRIPT  : create_bkp_dim_members.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Creation of hive TF work table gold_shared_dim.bkp_dim_members 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.bkp_dim_members
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
	market_key BIGINT,
	est_load_timestamp TIMESTAMP,
    utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/bkp_dim_members';