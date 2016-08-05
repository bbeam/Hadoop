--/*
--  HIVE SCRIPT  : create_dq_storefront_product_event_type.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_product_event_type). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/legacy/main/hive/create_dq_storefront_product_event_type.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_legacy="${SOURCE_legacy}"
--*/

--  Creating a DQ hive table(inc_storefront_product_event_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_product_event_type
(
	create_by STRING,
	create_date TIMESTAMP,     
	storefront_product_event_type_description STRING,   
	storefront_product_event_type_id INT,     
	storefront_product_event_type_name STRING,
	update_by STRING,
	update_date TIMESTAMP,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_storefront_product_event_type';