--/*
--  HIVE SCRIPT  : Create_ERR_DQ_t_Category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(ERR_DQ_t_Category) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a  hive table(ERR_T_Category) over the error data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.${hivevar:TABLE_ERR_DQ_T_CATEGORY}
(   
	CategoryId STRING,
	Name STRING,
	Status STRING,
	CreateDate STRING,
	CreateBy STRING,
	UpdateDate STRING,
	UpdateBy STRING,
	ErrorCode INT,
	ErrorDesc STRING
)
)
PARTITIONED BY (LoadDate STRING)
LOCATION '${hivevar:S3_LOCATION_GOLD_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:ALWEB_OPERATIONS_DB}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/${hivevar:TABLE_ERR_DQ_T_CATEGORY}';