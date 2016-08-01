--/*
--  HIVE SCRIPT  : create_dim_product_tmp.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 02, 2016
--  DESCRIPTION  : Creation of dim_product table in work db(dim_product_tmp). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.dim_product_tmp
(
	source_aK INT, 
	source_table STRING, 
	source_column STRING, 
	product_key BIGINT,
	master_product_group STRING, 
	product_type STRING, 
	product STRING, 
	unit_price DECIMAL(10,2), 
	source STRING, 
	joins_flag BOOLEAN, 
	renewals_flag BOOLEAN,
	eff_start_dt date,
	eff_end_dt date,
	current_active_ind char(1),
	md5_non_key_value string,
	md5_key_value string,
	edh_bus_month STRING
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_ALWEBMETRICS}/dim_product_tmp';