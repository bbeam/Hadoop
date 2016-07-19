--/*
--  HIVE SCRIPT  : create_bkp_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 02, 2016
--  DESCRIPTION  : Creation of backup table(bkp_dim_product)over dim_product table. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEBMETRICS_OPERATIONS};

--  Creating a back up hive table(bkp_dim_product) over the dim_product table data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEBMETRICS_OPERATIONS}.bkp_dim_product
(source_aK INT,
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
eff_start_dt DATE,
eff_end_dt DATE,
current_active_ind CHAR(1),
md5_non_key_value STRING,
md5_key_value STRING
)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SUBJECT_ALWEBMETRICS}/bkp_dim_product';