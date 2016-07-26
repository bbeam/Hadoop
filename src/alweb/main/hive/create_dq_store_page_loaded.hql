--/*
--  HIVE SCRIPT  : Create_DQ_Store_Page_Loaded.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(Store_Page_Loaded). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a DQ hive table(DQ_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_DQ_STORE_PAGE_LOADED}
(	
	id STRING,
	anonymous_id STRING,
	event_text STRING,
	sent_at TIMESTAMP,
	service_provider_id INT,
	user_id INT
	)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:WORK_DB}/${hivevar:TABLE_DQ_STORE_PAGE_LOADED}';  