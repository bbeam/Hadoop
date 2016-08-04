--/*
--  HIVE SCRIPT  : create_dq_storefront_product_event.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_product_event). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/legacy/main/hive/create_dq_storefront_product_event.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_legacy="${SOURCE_LEGACY}"
--*/

--  Creating a DQ hive table(inc_storefront_product_event) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_product_event
( 
	campaign_id STRING,
	create_by STRING,
	create_date TIMESTAMP,  
	expiration_date	TIMESTAMP,
	member_id INT,
	storefront_item_id INT,	
	storefront_product_event_id INT,
	storefront_product_event_type_id INT,     
	update_by STRING,
	update_date TIMESTAMP,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_storefront_product_event';