SET hive.exec.dynamic.partition.mode=non-strict;
-- ######################################################################################################### 
-- HIVE SCRIPT				:targetdim_members_load.hql
-- AUTHOR					:Hive script is auto generated with java utility.
-- DESCRIPTION				:Hive script will run after pig script for scd.
-- #########################################################################################################


use ${hivevar:GOLD_SHARED_DIM_DB};

-- =========Loading work table to target dimension table========
INSERT OVERWRITE TABLE ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME}
SELECT member_key,
		member_id,
		user_id,
		email,
		postal_code,
		pay_status,
		member_status,
		expiration_status,
		member_dt,
		membership_tier_nm,
		primary_phone_number,
		first_nm,
		last_nm,
		associate,
		employee,
		market_key,
		est_load_timestamp,
		utc_load_timestamp 
 FROM ${hivevar:WORK_SHARED_DIM_DB}.${hivevar:WORK_DIM_TABLE_NAME};

