--/*
--  HIVE SCRIPT  : Create_INC_t_Category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(t_Category). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB};

--  Creating a incoming hive table(INC_t_Category) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.${hivevar:TABLE_INC_T_CATEGORY}
(	CategoryId STRING,
	Name STRING,
	Status STRING,
	CreateDate STRING,
	CreateBy STRING,
	UpdateDate STRING,
	UpdateBy STRING
)PARTITION (LoadDate STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
LOCATION '{hivevar:S3_LOCATION_INCOMING_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:ALWEB_INCOMING_DB}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/${hivevar:TABLE_INC_T_CATEGORY}';