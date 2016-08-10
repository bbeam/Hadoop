--/*
--  HIVE SCRIPT  : create_dq_t_service_provider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 27, 2016
--  DESCRIPTION  : Creation of hive incoming DQtable(dq_t_service_provider). 
--*/
--  Creating a DQ hive table(dq_t_service_provider) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_service_provider
(	
	service_provider_id INT, 
	listing_id INT, 
	al_id INT, 
	name VARCHAR(254), 
	hours VARCHAR(254), 
	est_joined_date TIMESTAMP, 
	joined_date TIMESTAMP, 
	description STRING, 
	services_offered STRING, 
	service_provider_Type VARCHAR(254), 
	in_business_since INT, 
	is_national TINYINT, 
	status VARCHAR(254), 
	version INT, 
	est_create_date TIMESTAMP,
	create_date TIMESTAMP, 
	create_by INT, 
	est_update_date TIMESTAMP, 
	update_date TIMESTAMP, 
	update_by INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_service_provider';