--/*
--  HIVE SCRIPT  : Load_dim_product.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 04, 2016
--  DESCRIPTION  : The hive script loads the data into the dimension table 'dim_product' and increments the surrogate key for the next run. 
--*/

-- Turning off partition mode to non strict to allow dynamic partition without a sigle static partition.
SET hive.exec.dynamic.partition.mode=non-strict;


-- Insert into final dimention table from a temporary table. 
INSERT OVERWRITE TABLE ${hivevar:ALWEB_GOLD_DB}.${hivevar:TABLE_DIM_PRODUCT} 
	PARTITION(loadmonth) 
	SELECT source_ak, source_table, source_column, product_key, master_product_group, product_type, product, unit_price,
	source, joins_flag, renewals_flag, eff_start_dt, eff_end_dt, current_active_ind, md5_non_key_value, md5_key_value, 
	loadmonth 
	FROM 
	${hivevar:WORK_DB}.${hivevar:TABLE_DIM_PRODUCT_TMP} ; 


-- Reset the surrogate key to the max value of the last successful run, for the specific table_name 
INSERT OVERWRITE TABLE ${hivevar:ALWEB_GOLD_DB}.${hivevar:SK_MAP) 
	PARTITION(table_name) 
	SELECT MAX(product_key) AS s_key,'dim_product' 
	FROM 
	${hivevar:ALWEB_GOLD_DB}.${hivevar:TABLE_DIM_PRODUCT} ; 

