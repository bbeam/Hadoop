--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_storefront_item_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_item_type). 
--*/

--  Creating a dq hive table(dq_storefront_item_type) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_item_type
(
	storefront_item_type_id INT,
	storefront_item_type_name STRING,
	web_content_key STRING,
	receipt_email_object STRING,
	instructions_email_object STRING,
	service_provider_email_object STRING,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	storefront_item_type_default_fee DECIMAL(5,2),
	product_id INT,
	est_load_timestamp TIMESTAMP,
	ust_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_storefront_item_type';
