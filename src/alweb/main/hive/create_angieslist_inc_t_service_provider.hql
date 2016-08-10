-/*
--  HIVE SCRIPT  : create_angieslist_inc_t_service_provider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_service_provider). 
--


--  Creating a incoming hive table(inc_t_service_provider) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_service_provider
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
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_service_provider';