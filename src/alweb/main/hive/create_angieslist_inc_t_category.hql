--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_category.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_category). 
--*/

--  Creating a incoming hive table(inc_t_category) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_category
(
	category_id STRING,
	name STRING,
	status STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_category';