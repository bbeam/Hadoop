--/*
--  HIVE SCRIPT  : create_dq_report_sp_category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_report_sp_category).
--*/

--  Creating a dq hive table(dq_report_sp_category) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_report_sp_category
(
	category_id INT,
	create_by VARCHAR(50),
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	is_primary TINYINT,
	report_id INT,
	report_sp_category_id INT,
	sp_id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_report_sp_category';
