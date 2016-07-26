--/*
--  HIVE SCRIPT  : Create_ERR_DQ_Store_Page_Loaded.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 14, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(ERR_DQ_Store_Page_Loaded) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(T_SKU_SERDE) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.${hivevar:TABLE_ERR_DQ_STORE_PAGE_LOADED}
(	
	id STRING,
	anonymous_id STRING,
	event_text STRING,
	sent_at STRING,
	service_provider_id STRING,
	user_id STRING,
	error_type STRING,
	error_desc STRING,
	DQTimeStamp STRING
)
PARTITIONED BY (LoadDate STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/${hivevar:TABLE_ERR_DQ_STORE_PAGE_LOADED}';
