--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_product_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_product_type). 
--*/

--  Creating a dq hive table(dq_t_sku) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_product_type
(
	product_type_id INT,
	product_type_name VARCHAR(255),
	product_type_description VARCHAR(255),
	product_type_active TINYINT,
	product_type_is_primary TINYINT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_product_type';
