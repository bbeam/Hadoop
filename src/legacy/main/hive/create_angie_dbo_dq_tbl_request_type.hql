--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_tbl_requested_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requested_type).
--*/

--  Creating a dq hive table(dq_tbl_requested_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_tbl_request_type
(
	request_type_id SMALLINT ,
	request_type STRING,
	request_type_description STRING ,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_tbl_request_type';
