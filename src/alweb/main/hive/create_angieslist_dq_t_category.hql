--/*
--  HIVE SCRIPT  : create_dq_t_category.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_t_category). 
--*/

--  Creating a DQ hive table(inc_t_category) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_category
(
	category_id INT,
	name VARCHAR(254),
	status VARCHAR(254),
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_category';