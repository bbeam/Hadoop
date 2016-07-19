--/*
--  HIVE SCRIPT  : create_tf_dim_product.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive transformation table(tf_dim_product). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.tf_dim_product
(
	source_ak INT, 
	isemailpromotable INT, 
	product STRING, 
	unit_price DECIMAL(10,2)
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_ALWEBMETRICS}/tf_dim_product';