--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_tbl_requested_type_info.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requested_type_info). 
--*/


--  Creating a incoming hive table(inc_tbl_requested_type_info) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_tbl_request_type_info
(
	request_info_id STRING,
	request_id STRING,
	request_type_id STRING,
	cat_id STRING,
	keyword STRING,
	sp_id STRING,
	request_count STRING,
	bln_declined_alarm STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_tbl_request_type_info';
