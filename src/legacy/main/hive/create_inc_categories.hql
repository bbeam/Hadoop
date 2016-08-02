--/*
--  HIVE SCRIPT  : create_inc_categories.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_categories). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_categories.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_categories) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_categories
(
	category_id STRING,
	category_name STRING,
	category STRING,
	fundraiser_category_id STRING,
	rank STRING, 
	category_group_id STRING, 
	category_subgroup_id STRING, 
	seo_category_id STRING, 
	average_job_cost STRING, 
	is_active STRING, 
	category_travel_direction_id STRING, 
	checksum STRING

)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_categories';