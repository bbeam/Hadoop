--/*
--  HIVE SCRIPT  : Load_bkp_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 06, 2016
--  DESCRIPTION  : The hive script loads the data into the back up table 'bkp_dim_product'  
--*/

/* Insert into back up table from a final table . */
INSERT OVERWRITE TABLE ${hivevar:ALWEBMETRICS_OPERATIONS}.${hivevar:TABLE_BKP_DIM_PRODUCT}
SELECT source_ak, source_table, source_column, product_key, master_product_group, product_type, product, unit_price,
source, joins_flag, renewals_flag, eff_start_dt, eff_end_dt, current_active_ind, md5_non_key_value, md5_key_value
FROM 
${hivevar:ALWEB_GOLD_DB}.${hivevar:TABLE_DIM_PRODUCT} ;