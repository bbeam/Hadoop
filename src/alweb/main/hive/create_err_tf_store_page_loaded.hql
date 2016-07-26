--/*
--  HIVE SCRIPT  : Create_ERR_TF_Store_Page_Loadedt.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 14, 2016
--  DESCRIPTION  : Creation of hive transformation table(TF_Store_Page_Loadedt). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(TF_Store_Page_Loaded) over the transformaed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.${hivevar:TABLE_ERR_TF_STORE_PAGE_LOADED}
(
	id STRING,
	anonymous_id STRING,
	event_text STRING,
	sent_at TIMESTAMP,
	service_provider_id INT,
	user_id INT,
	error_type STRING,
	error_desc STRING,
	TFTimeStamp STRING
)
PARTITIONED BY (LoadDate STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SUBJECT_ALWEBMETRICS}/${hivevar:TABLE_ERR_TF_STORE_PAGE_LOADED}';