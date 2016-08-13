--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_tbl_requests.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requests). 
--*/


--  Creating a incoming hive table(inc_tbl_requests) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_tbl_requests
(
	request_id STRING,
	old_request_id STRING,
	request_date STRING,
	member_id STRING,
	employee_id STRING,
	counter STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_tbl_requests';