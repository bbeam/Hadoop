--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_sp_market.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_sp_market). 
--*/


--  Creating a incoming hive table(inc_sp_market) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_sp_market
(
	sp_market_id STRING,
	sp_id STRING,
	market_id STRING,
	primary_market STRING,
	create_date STRING,
	create_by STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_sp_market';