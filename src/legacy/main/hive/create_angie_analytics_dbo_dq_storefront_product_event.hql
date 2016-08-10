--/*
--  HIVE SCRIPT  : create_dq_storefront_product_event.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_product_event). 
--*/

--  Creating a DQ hive table(inc_storefront_product_event) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_product_event
( 
	storefront_product_event_id INT,
	storefront_item_id INT,
	member_id INT,
	storefront_product_event_type_id INT,
	est_expiration_date TIMESTAMP,
	expiration_date	TIMESTAMP,
	campaign_id STRING,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angieanalytics/dbo/full/daily/dq_storefront_product_event';
