--/*
--  HIVE SCRIPT  : create_inc_t_service_provider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_service_provider). 
--*/

-- Create the database if it does not exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB};

--  Creating a incoming hive table(inc_t_service_provider) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_service_provider
(
	service_provider_id STRING, 
	listing_id STRING, 
	al_id STRING, 
	name STRING, 
	hours STRING, 
	joined_date STRING, 
	description STRING, 
	services_offered STRING, 
	service_provider_type STRING, 
	in_business_since STRING, 
	is_national STRING, 
	status STRING, 
	version STRING, 
	create_date STRING, 
	create_by STRING, 
	update_date STRING, 
	update_by STRING
)
PARTITIONED BY (bus_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "\u0001",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_service_provider';





--/*
--  HIVE SCRIPT  : create_inc_t_service_provider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_service_provider). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_service_provider.hql \
--					--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/ 
