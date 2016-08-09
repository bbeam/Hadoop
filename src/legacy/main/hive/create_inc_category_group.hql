--/*
--  HIVE SCRIPT  : create_inc_category_group.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_category_group). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_category_group.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_category_group) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_category_group
(
	category_group STRING,
	category_group_description STRING,
	category_group_id STRING,
	category_group_is_active STRING,
	category_group_type_id STRING,
	display_order STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_category_group';