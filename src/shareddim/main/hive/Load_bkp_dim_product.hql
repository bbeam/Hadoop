--/*
--  HIVE SCRIPT  : Load_bkp_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 06, 2016
--  DESCRIPTION  : The hive script loads the data into the back up table 'bkp_dim_product'  
--*/


/* Turning off partition mode to non strict to allow dynamic partition without a sigle static partition. */
SET hive.exec.dynamic.partition.mode=non-strict;


/* Insert into back up table from a final table . */
INSERT OVERWRITE TABLE ${hivevar:ALWEBMETRICS_OPERATIONS}.${hivevar:TABLE_BKP_DIM_PRODUCT}
PARTITION(loadmonth)
SELECT * FROM ${hivevar:ALWEB_GOLD_DB}.${hivevar:TABLE_DIM_PRODUCT} ;