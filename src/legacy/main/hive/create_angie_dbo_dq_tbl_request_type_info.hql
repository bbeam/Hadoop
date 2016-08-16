--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_tbl_requested_type_info.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requested_type_info). 
--*/

--  Creating a dq hive table(dq_tbl_requested_type_info) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_tbl_request_type_info
(
	request_info_id INT,
	request_id INT,
	request_type_id SMALLINT,
	cat_id INT,
	keyword STRING,
	sp_id INT,
	request_count SMALLINT,
	bln_declined_alarm SMALLINT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_tbl_request_type_info';
