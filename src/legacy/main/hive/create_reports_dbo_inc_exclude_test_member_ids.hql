--/*
--  HIVE SCRIPT  : create_reports_dbo_inc_exclude_test_member_ids.hql
--  AUTHOR       : Varun 
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_exclude_test_member_ids). 
--*/

--  Creating a incoming hive table(inc_exclude_test_member_ids) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_exclude_test_member_ids
(
	member_id STRING,
 	date_added STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/reports/dbo/full/daily/inc_exclude_test_member_ids';