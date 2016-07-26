--/*
--  HIVE SCRIPT  : Create_TF_Store_Page_Loaded.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive transformation table(TF_Store_Page_Loaded). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a incoming hive table(TF_Store_Page_Loaded) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_TF_STORE_PAGE_LOADED}
(	
	id STRING,
	anonymous_id STRING,
	event_text STRING,
	sent_at TIMESTAMP,
	service_provider_id INT,
	user_id INT
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_ALWEBMETRICS}/${hivevar:TABLE_TF_STORE_PAGE_LOADED}';