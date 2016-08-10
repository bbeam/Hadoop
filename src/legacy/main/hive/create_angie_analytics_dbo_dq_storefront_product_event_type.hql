--/*
--  HIVE SCRIPT  : create_angie_analytics_dbo_dq_storefront_product_event_type.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_product_event_type). 
--*/

--  Creating a DQ hive table(inc_storefront_product_event_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_product_event_type
(
	storefront_product_event_type_id INT,
	storefront_product_event_type_name STRING,
	storefront_product_event_type_description STRING,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,	
	update_by STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angieanalytics/dbo/full/daily/dq_storefront_product_event_type';
