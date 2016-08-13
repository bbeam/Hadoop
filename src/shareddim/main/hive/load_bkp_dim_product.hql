SET hive.exec.dynamic.partition.mode=non-strict;
--
--  HIVE SCRIPT  : load_bkp_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 06, 2016
--  DESCRIPTION  : This script would take a backup copy of the target table 
--


-- Insert into back up table from a final table .
INSERT OVERWRITE TABLE ${hivevar:TARGET_DB}.bkp_dim_product
PARTITION(edh_bus_month)
SELECT * FROM ${hivevar:SOURCE_DB}.dim_product;