SET hive.exec.dynamic.partition.mode=non-strict;
--
--  HIVE SCRIPT  : load_bkp_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 06, 2016
--  DESCRIPTION  : The hive script loads the data into the back up table 'bkp_dim_product'  
--


-- Insert into back up table from a final table .
INSERT OVERWRITE TABLE ${hivevar:SHAREDDIM_OPERATIONS_DB}.bkp_dim_product
PARTITION(bus_month)
SELECT * FROM ${hivevar:SHAREDDIM_GOLD_DB}.dim_product;