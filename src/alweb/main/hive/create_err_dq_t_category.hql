--/*
--  HIVE SCRIPT  : create_err_dq_t_category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(err_dq_t_category) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a  hive table(ERR_T_Category) over the error data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.err_dq_t_category
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
PARTITIONED BY (bus_date STRING)
LOCATION '${hivevar:S3_LOCATION_GOLD_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:ALWEB_OPERATIONS_DB}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/err_dq_t_category';