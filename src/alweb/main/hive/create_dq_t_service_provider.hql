--/*
--  HIVE SCRIPT  : create_dq_t_service_provider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 27, 2016
--  DESCRIPTION  : Creation of hive incoming DQtable(dq_t_service_provider). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_t_service_provider.hql \
--					--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/



-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB};

--  Creating a DQ hive table(dq_t_service_provider) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_service_provider
(	
	service_provider_id BIGINT, 
	listing_id BIGINT, 
	al_id BIGINT, 
	name VARCHAR(254), 
	hours VARCHAR(254), 
	joined_date TIMESTAMP, 
	description STRING, 
	services_offered STRING, 
	service_provider_Type VARCHAR(254), 
	in_business_since INT, 
	is_national TINYINT, 
	status VARCHAR(254), 
	version BIGINT, 
	create_date TIMESTAMP, 
	create_by BIGINT, 
	update_date TIMESTAMP, 
	update_by BIGINT,
	load_timestamp TIMESTAMP
)
STORED AS ORC
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_service_provider';