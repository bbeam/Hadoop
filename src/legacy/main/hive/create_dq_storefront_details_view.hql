--/*
--  HIVE SCRIPT  : create_dq_storefront_details_view.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_details_view). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_storefront_details_view.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_storefront_details_view) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_storefront_details_view
(
	store_front_details_view_id INT,
	store_front_item_id INT,
	member_id INT,
	store_front_source_id INT,
	create_date TIMESTAMP,
	create_by STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_storefront_details_view';