--/*
--  HIVE SCRIPT  : create_dim_category.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive TF work table gold_shared_dim.dim_category 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_category
(
   category_key BIGINT,
   category_id INT,
   category STRING,
   legacy_category STRING,
   new_world_category STRING,
   additional_category_nm STRING,
   is_active BOOLEAN,
   category_group STRING,
   category_group_type STRING
)
PARTITIONED BY (edh_bus_month STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_category';