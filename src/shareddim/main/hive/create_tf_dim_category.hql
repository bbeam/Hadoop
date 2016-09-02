--/*
--  HIVE SCRIPT  : create_tf_dim_category.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.tf_dim_category 
--
--
-- hive -f $CREATE_TF_HQL_PATH \
-- -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
-- -hivevar TF_TABLE_NAME=$TF_TABLE_NAME
--*/

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME}
(
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
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/tf_dim_category';