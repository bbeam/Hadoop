--/*
--  HIVE SCRIPT  : create_scd_dim_category.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 18, 2016
--  DESCRIPTION  : Creation of hive SCD work table
--
-- hive -f $CREATE_SCD_HQL_PATH \
-- -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
-- -hivevar WORK_DIM_TABLE_NAME=$WORK_DIM_TABLE_NAME
--*/

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME}
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
   action_cd STRING,
   est_load_timestamp TIMESTAMP,
   utc_load_timestamp TIMESTAMP
)
LOCATION '/user/hadoop/data/work/shareddim/scd_dim_category';