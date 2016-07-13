--/*
--  HIVE SCRIPT  : Create_EDH_Batch_Audit.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 08, 2016
--  DESCRIPTION  : Creation of Audit hive table . 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:COMMON_OPERATIONS_DB};

--  Creating a incoming hive table(EDH_BATCH_AUDIT) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:COMMON_OPERATIONS_DB}.${hivevar:TABLE_EDH_BATCH_AUDIT}
(Load_Date STRING,
Entity STRING,
Table_Name STRING,
Process STRING,
Type STRING,
Sub_Type STRING,
Record_Count STRING,
Time_Stamp STRING,
User_Name STRING) 
PARTITIONED BY (Load_Month STRING)
STORED AS ORC 
LOCATION '${hivevar:S3_DATA_LOCATION_COMMON_OPERATIONS}/${hivevar:TABLE_EDH_BATCH_AUDIT}';