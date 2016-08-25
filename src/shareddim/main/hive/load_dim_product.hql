SET hive.exec.dynamic.partition.mode=non-strict;
-- ######################################################################################################### 
-- HIVE SCRIPT				:targetdim_product_load.hql
-- AUTHOR					:Hive script is auto generated with java utility.
-- DESCRIPTION				:Hive script will run after pig script for scd.
-- #########################################################################################################


use ${hivevar:GOLD_SHARED_DIM_DB};

-- =========Loading work table to target dimension table========
INSERT OVERWRITE TABLE ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME}
SELECT product_key,
		source_ak,
		source_table,
		source_column,
		master_product_group,
		product_type,
		product,
		unit_price,
		source,
		est_load_timestamp,
		utc_load_timestamp 
 FROM ${hivevar:WORK_SHARED_DIM_DB}.${hivevar:WORK_DIM_TABLE_NAME};

