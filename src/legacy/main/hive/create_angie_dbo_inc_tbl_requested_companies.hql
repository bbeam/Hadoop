--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_tbl_requested_companies.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requested_companies). 
--*/


--  Creating a incoming hive table(inc_tbl_requested_companies) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_tbl_requested_companies
(
gave_id STRING,
request_info_id STRING,
requested_grade_id STRING,
sp_id STRING,
category_id STRING,
gave_date STRING,
original_requested_id STRING,
gave_count STRING,
member_id STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/incremental/daily/inc_tbl_requested_companies';