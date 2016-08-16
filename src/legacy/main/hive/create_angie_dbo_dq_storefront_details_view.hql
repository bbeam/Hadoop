--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_storefront_details_view.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_storefront_details_view). 
--*/

--  Creating a dq hive table(dq_storefront_details_view) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_storefront_details_view
(
	store_front_details_view_id INT,
	store_front_item_id INT,
	member_id INT,
	store_front_source_id INT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/incremental/daily/dq_storefront_details_view';
