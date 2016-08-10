--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_product_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_product_type). 
--*/


--  Creating a incoming hive table(inc_producttype) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_product_type
(
	product_type_id STRING,
	product_type_name STRING,
	product_type_description STRING,
	product_type_active STRING,
	product_type_is_primary STRING

)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_product_type';