--/*
--  HIVE SCRIPT  : create_inc_markets.hql
--  AUTHOR       : Gaurav maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_markets). 
--*/

--  Creating a incoming hive table(inc_markets) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_markets
(
	market_id STRING,    
	market STRING,     
	address_1 STRING,   
	address_2 STRING,     
	city STRING,
	state STRING,
	zip STRING,
	phone_1 STRING,
	phone_2 STRING,
	fax STRING,
	email STRING,
	email_2 STRING,
	email_3 STRING,
	current_days  STRING,
	mm_first STRING,
	mm_last STRING,
	phone_3 STRING,
	phone_4 STRING,
	phone_5 STRING,
	search_flag STRING,
	bucket STRING,
	int_detailed_sort STRING,
	int_display_requirement STRING,
	marketplace_email STRING,
	status STRING,
	fundraiser_phone STRING,
	fundraiser_email STRING,
	new_market_check STRING,
	market_open_date STRING,
	charter_end_date STRING,
	alm  STRING,
	toll_free_phone STRING,
	toll_free_fax  STRING,
	default_radius STRING,
	country_code_id STRING,
	market_url STRING,
	big_deal STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_markets';