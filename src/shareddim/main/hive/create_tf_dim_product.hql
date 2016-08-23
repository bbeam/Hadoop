--/*
--  HIVE SCRIPT  : create_tf_dim_product.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 20, 2016
--  DESCRIPTION  : Creation of tf_dim_product table in work db. 
--*/

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.tf_dim_product
(
source_aK INT,
source_table STRING,
source_column STRING,
master_product_group STRING, 
product_type STRING, 
product STRING, 
unit_price DECIMAL(10,2), 
source STRING
)
LOCATION '$WORK_DIR/data/work/shareddim/tf_dim_product';