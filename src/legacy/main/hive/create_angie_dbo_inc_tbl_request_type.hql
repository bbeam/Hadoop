--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_tbl_requested_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requested_type). 
--*/


--  Creating a incoming hive table(inc_tbl_requested_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_tbl_request_type
(
	request_type_id STRING,
	request_type STRING,
	request_type_description STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_tbl_request_type';
