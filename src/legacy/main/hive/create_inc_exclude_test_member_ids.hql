--/*
--  HIVE SCRIPT  : create_inc_exclude_test_member_ids.hql
--  AUTHOR       : Varun 
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_exclude_test_member_ids). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_exclude_test_member_ids.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_exclude_test_member_ids) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_exclude_test_member_ids
(
	member_id STRING,
 	date_added STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/reports/full/daily/inc_exclude_test_member_ids';