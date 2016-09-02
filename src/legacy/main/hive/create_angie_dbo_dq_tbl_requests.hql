--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_tbl_requests.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requests). 
--*/

--  Creating a dq hive table(dq_tbl_requests) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_tbl_requests
(
	request_id INT,
	old_request_id INT,
	est_request_date TIMESTAMP,
	request_date TIMESTAMP,
	member_id INT,
	employee_id INT,
	counter SMALLINT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/incremental/daily/dq_tbl_requests';