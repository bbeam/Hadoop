--/*
--  HIVE SCRIPT  : create_tf_dim_market.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive TF work table.
--  Execute command:
--
--
-- hive -f $CREATE_TF_HQL_PATH \
-- -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
-- -hivevar TF_TABLE_NAME=$TF_TABLE_NAME
--*/

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME}
(
   market_nm string,
   market_id int,
   est_load_timestamp TIMESTAMP,
   utc_load_timestamp TIMESTAMP

)
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/tf_dim_market';