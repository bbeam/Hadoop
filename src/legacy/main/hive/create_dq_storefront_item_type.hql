--/*
--  HIVE SCRIPT  : create_dq_storefront_item_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_item_type). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_storefront_item_type.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}" 
--*/

--  Creating a dq hive table(dq_storefront_item_type) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_item_type
(
	storefront_item_type_id INT,
	storefront_item_type_name STRING,
	web_content_key STRING,
	receipt_email_object STRING,
	instructions_email_object STRING,
	service_provider_email_object STRING,
	create_date TIMESTAMP,
	create_by STRING,
	storefront_item_type_default_fee DECIMAL(5,2),
	product_id INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_storefront_item_type';