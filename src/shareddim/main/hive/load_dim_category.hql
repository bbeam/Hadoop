SET hive.exec.dynamic.partition.mode=non-strict;
-- ######################################################################################################### 
-- HIVE SCRIPT				:targetdim_category_load.hql
-- AUTHOR					:Hive script is auto generated with java utility.
-- DESCRIPTION				:Hive script will run after pig script for scd.
-- #########################################################################################################


use ${hivevar:GOLD_SHARED_DIM_DB};

-- =========Loading work table to target dimension table========
INSERT OVERWRITE TABLE ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME}
SELECT category_key,
		category_id,
		category,
		legacy_category,
		new_world_category,
		additional_category_nm,
		is_active,
		category_group,
		category_group_type,
		est_load_timestamp,
		utc_load_timestamp 
 FROM ${hivevar:WORK_SHARED_DIM_DB}.${hivevar:WORK_DIM_TABLE_NAME};

