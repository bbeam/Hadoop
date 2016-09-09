--/*
--  HIVE SCRIPT  : create_tf_dim_member.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.tf_dim_member
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/shareddim/main/hive/create_tf_dim_member.hql \
-- -hivevar HIVE_DB=$WORK_SHARED_DIM_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET 
--*/
DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME}
(
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
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/tf_dim_member';