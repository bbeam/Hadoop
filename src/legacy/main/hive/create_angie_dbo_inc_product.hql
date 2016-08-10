--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_product). 
--*/


--  Creating a incoming hive table(inc_product) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_product
(
	product_id STRING,
	product_name STRING,
	product_type_id STRING,
	active_ind STRING,
	comments STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_product';