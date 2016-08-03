--/*
--  HIVE SCRIPT  : create_dq_markets.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 09, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_markets). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_dq_markets.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_legacy="${SOURCE_legacy}"
--*/

--  Creating a DQ hive table(inc_markets) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_markets
(
	market_id INT,    
	market STRING,     
	address1 STRING,    
	address2 STRING,     
	city STRING,
	state STRING,
	zip STRING,
	phone1 STRING,
	phone2 STRING,
	fax STRING,
	email STRING,
	email2 STRING,
	email3 STRING,
	current_days  INT,
	mm_first STRING,
	mm_last STRING,
	phone3 STRING,
	phone4 STRING,
	phone5 STRING,
	search_flag SMALLINT,
	bucket INT,
	int_detailed_sort SMALLINT,
	int_display_requirement INT,
	market_place_email STRING,
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_markets';