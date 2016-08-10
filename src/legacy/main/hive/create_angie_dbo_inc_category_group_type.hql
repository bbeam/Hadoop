--/*
--  HIVE SCRIPT  : create_inc_category_group_type.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_category_group_type). 
--*/

--  Creating a incoming hive table(inc_category_group_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_category_group_type
(
	category_group_type_id STRING,
	category_group_type STRING,
	category_group_type_description STRING,
	category_group_type_active STRING,
	category_group_type_name STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_category_group_type';