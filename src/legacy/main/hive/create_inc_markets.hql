--/*
--  HIVE SCRIPT  : create_inc_markets.hql
--  AUTHOR       : Gaurav maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_markets). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_markets.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_markets) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_markets
(
	market_id STRING,    
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
	current_days  STRING,
	mm_first STRING,
	mm_last STRING,
	phone3 STRING,
	phone4 STRING,
	phone5 STRING,
	search_flag STRING,
	bucket STRING,
	int_detailed_sort STRING,
	int_display_requirement STRING,
	market_place_email STRING,
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_markets';