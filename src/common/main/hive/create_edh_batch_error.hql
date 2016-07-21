--/*
--  HIVE SCRIPT  : create_edh_batch_error.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 15, 2016
--  DESCRIPTION  : Creation of Error table  
--*/


--  Creating a table(EDH_BATCH_ERROR) over the error data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:COMMON_OPERATIONS_DB}.edh_batch_error
(entity  STRING,
process STRING, 
error_type STRING, 
error_desc STRING, 
record_header STRING,
error_record STRING,
time_stamp TIMESTAMP, 
user_name  STRING)     
PARTITIONED BY (table_name STRING,bus_date DATE)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/common/edh_batch_error';