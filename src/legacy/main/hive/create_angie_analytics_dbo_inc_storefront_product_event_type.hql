--/*
--  HIVE SCRIPT  : create_angie_analytics_dbo_inc_storefront_product_event_type.hql
--  AUTHOR       : Gaurav maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_storefront_product_event_type). 
--*/

--  Creating a incoming hive table(inc_storefront_product_event_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_storefront_product_event_type
(
	 
	storefront_product_event_type_id STRING,     
	storefront_product_event_type_name STRING,   
	storefront_product_event_type_description STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
	
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angieanalytics/dbo/full/daily/inc_storefront_product_event_type';