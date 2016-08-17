--/*
--  HIVE SCRIPT  : create_tf_dim_market.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.tf_dim_members
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/shareddim/main/hive/create_tf_dim_members.hql \
-- -hivevar HIVE_DB=$WORK_SHARED_DIM_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET 
--*/

CREATE TABLE IF NOT EXISTS ${hivevar:HIVE_DB}.tf_dim_members
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
	market_key STRING
)
LOCATION '/user/hive/warehouse/work_shared_dim.db/tf_dim_members';