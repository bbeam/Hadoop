--/*
--  HIVE SCRIPT  : create_bkp_dim_category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of bkp_dim_category table in operations db 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.bkp_dim_category
(
   category_key BIGINT,
   category_id INT,
   category STRING,
   legacy_category STRING,
   new_world_category STRING,
   additional_category_nm STRING,
   is_active BOOLEAN,
   category_group STRING,
   category_group_type STRING,
   est_load_timestamp TIMESTAMP,
   utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/operations/shareddim/bkp_dim_category';