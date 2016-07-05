--/*
--  HIVE SCRIPT  : Create_DQ_t_Category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : JUN 23, 2016
--  DESCRIPTION  : Creating a hive table(DQ_T_CATEGORY) for incoming data  
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a DQ hive table(DQ_T_CATEGORY) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_DQ_T_CATEGORY}
(
	CategoryId INT,
	Name STRING,
	Status STRING,
	CreateDate TIMESTAMP,
	CreateBy INT,
	UpdateDate TIMESTAMP,
	UpdateBy INT
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:WORK_DB}/${hivevar:TABLE_DQ_T_CATEGORY}';