--/*
--  HIVE SCRIPT  : create_work_dim_market.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.dim_market 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.scd_dim_market
(
   market_key BIGINT,
   market_nm STRING,
   market_id INT,
   action_cd STRING,
   edh_bus_month STRING
)
LOCATION 'hdfs://ip-172-26-40-211.ec2.internal:8020/user/hadoop/data/work/shareddim/scd_dim_market';