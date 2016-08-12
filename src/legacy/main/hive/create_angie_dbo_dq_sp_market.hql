--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_sp_market.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_sp_market). 
--*/

--  Creating a dq hive table(dq_sp_market) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_sp_market
(
	sp_market_id INT,
	sp_id INT,
	market_id INT,
	primary_market TINYINT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_sp_market';