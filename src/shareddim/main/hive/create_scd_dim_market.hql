--/*
--  HIVE SCRIPT  : create_scd_dim_market.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive SCD work table
--
-- hive -f $CREATE_SCD_HQL_PATH \
-- -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
-- -hivevar WORK_DIM_TABLE_NAME=$WORK_DIM_TABLE_NAME
--*/

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME}
(
   market_key BIGINT,
   market_nm STRING,
   market_id INT,
   action_cd STRING,
   est_load_timestamp TIMESTAMP,
   utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/scd_dim_market';