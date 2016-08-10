--/*
--  HIVE SCRIPT  : create_dq_report_sp_category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_report_sp_category).
--*/

--  Creating a dq hive table(dq_report_sp_category) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_report_sp_category
(
	report_sp_category_id INT,  
	report_id INT,
	sp_id INT, 
	category_id INT,
	is_primary TINYINT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_report_sp_category';
