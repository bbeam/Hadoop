--/*
--  HIVE SCRIPT  : Create_BR_dim_product.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 02, 2016
--  DESCRIPTION  : Creation of dim_product table in work db(BR_dim_product). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_BR_DIM_PRODUCT}
(
source_aK INT,
source_table STRING,
source_column STRING,
master_product_group STRING, 
product_type STRING, 
product STRING, 
unit_price DECIMAL(10,2), 
source STRING, 
joins_flag BOOLEAN, 
renewals_flag BOOLEAN
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_ALWEBMETRICS}/${hivevar:WORK_DB}/${hivevar:TABLE_BR_DIM_PRODUCT}';