--/*
--  HIVE SCRIPT  : create_scd_dim_member.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive SCD work table
--
-- hive -f create_scd_dim_member.hql \
-- -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
-- -hivevar WORK_DIM_TABLE_NAME=$WORK_DIM_TABLE_NAME
--*/

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME}
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
   	action_cd STRING,
   	est_load_timestamp TIMESTAMP,
    utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/scd_dim_member';