--/*
--  HIVE SCRIPT  : Create_TF_dim_product.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive transformation table(TF_dim_product). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_TF_DIM_PRODUCT}
(
	source_aK INT, 
	source_table STRING, 
	source_column STRING, 
	master_product_group STRING, 
	product_type STRING, 
	Product STRING, 
	unit_price DECIMAL(10,2), 
	Source STRING, 
	Joins_Flag BOOLEAN, 
	Renewals_Flag BOOLEAN
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_ALWEBMETRICS}/${hivevar:WORK_DB}/${hivevar:TABLE_TF_DIM_PRODUCT}';