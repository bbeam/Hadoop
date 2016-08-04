--/*
--  HIVE SCRIPT  : create_dq_exclude_test_member_ids.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 04, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_exclude_test_member_ids). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_dq_exclude_test_member_ids.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}"
--*/

--  Creating a DQ hive table(inc_exclude_test_member_ids) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_exclude_test_member_ids
(
	member_id int,
 	date_added TIMESTAMP,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/reports/full/daily/dq_exclude_test_member_ids';