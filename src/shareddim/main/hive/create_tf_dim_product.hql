--/*
--  HIVE SCRIPT  : create_tf_dim_product.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 20, 2016
--  DESCRIPTION  : Creation of tf_dim_product table in work db. 
--*/
--  Creating a incoming hive table(tf_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.tf_dim_product
(
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
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/tf_dim_product';