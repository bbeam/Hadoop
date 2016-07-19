--/*
--  HIVE SCRIPT  : create_BKP_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 02, 2016
--  DESCRIPTION  : Creation of backup table(bkp_dim_product)over dim_product table. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS test; ${hivevar:SHAREDDIM_OPERATIONS_DB};

--  Creating a back up hive table(bkp_dim_product) over the dim_product table data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SHAREDDIM_OPERATIONS_DB}.bkp_dim_product
(source_aK INT,
source_table STRING,
source_column STRING,
product_key BIGINT,
master_product_group STRING,
product_type STRING,
product STRING,
unit_price DECIMAL(10,2),
source STRING
)PARTITIONED BY (bus_month STRING)
STORED AS ORC
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SUBJECT_SHAREDDIM}/bkp_dim_product';