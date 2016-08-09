--/*
--  HIVE SCRIPT  : create_inc_category_group_type.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_category_group_type). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_category_group_type.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_category_group_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_category_group_type
(
	category_group_type_id STRING,
	category_group_type STRING,
	category_group_type_description STRING,
	category_group_type_active STRING,
	category_group_type_name STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_category_group_type';