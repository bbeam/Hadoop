--/*
--  HIVE SCRIPT  : create_br_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 02, 2016
--  DESCRIPTION  : Creation of dim_product table in work db(BR_dim_product). 
--*/

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.br_dim_product
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
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_SHAREDDIM}/br_dim_product';