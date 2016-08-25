--/*
--  HIVE SCRIPT  : create_bkp_dim_market.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of bkp_dim_market table in operations db 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.bkp_dim_market
(
   market_key BIGINT,
   market_nm STRING,
   market_id INT,
   est_load_timestamp TIMESTAMP,
   utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/operations/shareddim/bkp_dim_market';