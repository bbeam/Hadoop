--/*
--  HIVE SCRIPT  : create_dq_categories.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_categories). 
--*/

--  Creating a DQ hive table(inc_categories) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_categories
(
	category_id INT,
	category_name STRING,
	category STRING,
	fundraiser_category_id INT,
	rank INT, 
	category_group_id INT, 
	category_subgroup_id INT, 
	seo_category_id INT, 
	average_job_cost DECIMAL(18,2), 
	is_active TINYINT, 
	category_travel_direction_id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_categories';
