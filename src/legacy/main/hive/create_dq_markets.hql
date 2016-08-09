--/*
--  HIVE SCRIPT  : create_dq_markets.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 09, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_markets). 
--*/

--  Creating a DQ hive table(inc_markets) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_markets
(
	market_id INT,    
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
	current_days  INT,
	mm_first STRING,
	mm_last STRING,
	phone_3 STRING,
	phone_4 STRING,
	phone_5 STRING,
	search_flag SMALLINT,
	bucket INT,
	int_detailed_sort SMALLINT,
	int_display_requirement INT,
	marketplace_email STRING,
	status INT,
	fundraiser_phone STRING,
	fundraiser_email STRING,
	new_market_check TINYINT,
	market_open_date TIMESTAMP,
	charter_end_date TIMESTAMP,
	alm  TINYINT,
	toll_free_phone STRING,
	toll_free_fax  STRING,
	default_radius INT,
	country_code_id INT,
	market_url STRING,
	big_deal TINYINT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_markets';
