--/*
--  HIVE SCRIPT  : dim_event_type.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 30, 2016
--  DESCRIPTION  : Loading  data into table (dim_event_type) . 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_event_type
(
    event_type_key INT,
	event_type STRING,
	search_type STRING,
	event_source STRING,
	event_sub_source STRING  
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_event_type';