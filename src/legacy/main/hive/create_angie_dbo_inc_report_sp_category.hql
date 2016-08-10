--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_report_sp_category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_report_sp_category). 
--*/


--  Creating a incoming hive table(inc_report_sp_category) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_report_sp_category
(
	category_id STRING,
	create_by STRING,
	create_date STRING,
	is_primary STRING,
	report_id STRING,
	report_sp_category_id STRING,
	sp_id STRING

)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_report_sp_category';