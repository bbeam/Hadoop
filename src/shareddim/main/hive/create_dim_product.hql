--/*
--  HIVE SCRIPT  : create_dim_product.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 23, 2016
--  DESCRIPTION  : Creation of dim_product table in gold db. 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_product
(
product_key BIGINT,
source_ak INT,
source_table STRING,
source_column STRING,
master_product_group STRING, 
product_type STRING, 
product STRING, 
unit_price DECIMAL(10,2), 
source STRING,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_product';