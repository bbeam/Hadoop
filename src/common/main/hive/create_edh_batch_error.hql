--/*
--  HIVE SCRIPT  : create_edh_batch_error.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 15, 2016
--  DESCRIPTION  : Creation of Error table  
--*/


--  Creating a table(EDH_BATCH_ERROR) over the error data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.edh_batch_error
(entity  STRING,
process STRING, 
error_type STRING, 
error_desc STRING, 
record_header STRING,
error_record STRING,
est_time_stamp STRING,
time_stamp STRING, 
user_name  STRING)     
PARTITIONED BY (table_name STRING,edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/operations/common/edh_batch_error';
