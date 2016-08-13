--/*
--  HIVE SCRIPT  : create_dim_market.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.dim_market 
--*/

CREATE TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_market
(
	market_key 	int,
	market_id	int,
	market_nm	string
);