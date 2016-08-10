--/*
--  HIVE SCRIPT  : create_reports_dbo_dq_exclude_test_member_ids.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 04, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_exclude_test_member_ids). 
--*/

--  Creating a DQ hive table(inc_exclude_test_member_ids) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_exclude_test_member_ids
(
	member_id int,
 	date_added TIMESTAMP,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/reports/dbo/full/daily/dq_exclude_test_member_ids';
