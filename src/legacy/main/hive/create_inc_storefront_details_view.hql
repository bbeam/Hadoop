--/*
--  HIVE SCRIPT  : create_inc_storefront_details_view.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_storefront_details_view). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_storefront_details_view.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_storefront_details_view) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_storefront_details_view
(
	store_front_details_view_id STRING,
	store_front_item_id STRING,
	member_id STRING,
	store_front_source_id STRING,
	create_date STRING,
	create_by STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_storefront_details_view';