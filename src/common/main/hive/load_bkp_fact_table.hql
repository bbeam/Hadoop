SET hive.exec.dynamic.partition.mode=non-strict;
--
--  HIVE SCRIPT  : load_bkp_fact_table.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 06, 2016
--  DESCRIPTION  : The hive script loads the data into the back up of fact table  
--


-- Insert into back up table from a final table .
INSERT OVERWRITE TABLE ${hivevar:BKP_DB_NAME}.${hivevar:BKP_TABLE_NAME}
PARTITION(${hivevar:PARTITION_COLUMNS})
SELECT * FROM ${hivevar:SOURCE_DB_NAME}.${SOURCE_TABLE_NAME};