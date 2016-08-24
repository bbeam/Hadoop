SET hive.exec.dynamic.partition.mode=non-strict;
-- ######################################################################################################### 
-- HIVE SCRIPT				:targetdim_service_provider_load.hql
-- AUTHOR					:Hive script is auto generated with java utility.
-- DESCRIPTION				:Hive script will run after pig script for scd.
-- #########################################################################################################


use ${hivevar:GOLD_SHARED_DIM_DB};

-- =========Loading work table to target dimension table========
INSERT OVERWRITE TABLE ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME}
SELECT service_provider_key,
		legacy_spid,
		new_world_spid,
		company_nm,
		service_provider_group_type,
		entered_dt,
		city,
		state,
		postal_code,
		is_excluded,
		web_advertiser,
		call_center_advertiser,
		pub_advertiser,
		is_insured,
		is_bonded,
		is_licensed,
		background_check,
		ecommerce_status,
		vintage,
		market_key,
		est_load_timestamp,
		utc_load_timestamp 
 FROM ${hivevar:WORK_SHARED_DIM_DB}.${hivevar:WORK_DIM_TABLE_NAME};

