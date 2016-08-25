SET hive.exec.dynamic.partition.mode=non-strict;
--
--  HIVE SCRIPT  : load_bkp_dim_table.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 06, 2016
--  DESCRIPTION  : This script would take a backup copy of the target table 
--


-- Insert into back up table from a final table .
INSERT OVERWRITE TABLE ${hivevar:BKP_DB_NAME}.${hivevar:BKP_TABLE_NAME}
SELECT * FROM ${hivevar:SOURCE_DB_NAME}.${hivevar:SOURCE_TABLE_NAME};