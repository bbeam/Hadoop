--/*
--  HIVE SCRIPT  : Create_INC_Store_Page_Loaded.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(Store_Page_Loaded). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB};

--  Creating a incoming hive table(INC_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.${hivevar:TABLE_INC_STORE_PAGE_LOADED}
(
	id String,
	received_at STRING,
	uuid STRING,
	anonymous_id STRING,
	context_ip STRING,
	context_library_name STRING,
	context_library_version STRING,
	context_page_path STRING,
	context_page_referrer STRING,
	context_page_search STRING,
	context_page_title STRING,
	context_page_url STRING,
	context_user_agent STRING,
	description STRING,
	event STRING,
	event_text STRING,
	event_type STRING,
	original_timestamp STRING,
	sent_at STRING,
	service_provider_id STRING,
	service_provider_name STRING,
	service_provider_status STRING,
	timestamp STRING,
	user_id STRING,
	user_primary_ad_zone STRING,
	user_primary_zip_code STRING,
	user_selected_ad_zone STRING,
	user_selected_zip_code STRING,
	uuid_ts STRING,
	context_campaign_medium STRING,
	context_campaign_name STRING,
	context_campaign_source STRING)
PARTITIONED BY (LoadDate STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
LOCATION '${hivevar:S3_LOCATION_INCOMING_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA_SEGMENT}/${hivevar:EXTRACTIONTYPE_INCREMENTAL}/${hivevar:FREQUENCY_DAILY}/${hivevar:TABLE_INC_STORE_PAGE_LOADED}';
