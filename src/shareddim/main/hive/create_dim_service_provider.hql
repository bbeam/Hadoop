--/*
--  HIVE SCRIPT  : create_dim_service_provider.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive TF work table gold_shared_dim.dim_service_provider 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_service_provider
(
	service_provider_key bigint,
	legacy_spid int,
	new_world_spid  int,
	company_nm  string,
	service_provider_group_type string,
	entered_dt  timestamp,
	city    string,
	state   string, 
	postal_code string,
	is_excluded tinyint,
	web_advertiser  tinyint,
	call_center_advertiser  tinyint,
	pub_advertiser  tinyint,
	is_insured  tinyint,
	is_bonded   tinyint,
	is_licensed tinyint,
	background_check  tinyint,
	ecommerce_status tinyint,
	vintage string,
	market_key int,
    est_load_timestamp TIMESTAMP,
    utc_load_timestamp TIMESTAMP	
)
PARTITIONED BY (edh_bus_month STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_service_provider';