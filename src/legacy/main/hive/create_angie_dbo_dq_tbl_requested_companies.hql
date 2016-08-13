--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_tbl_requested_companies.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requested_companies). 


--*/

--  Creating a dq hive table(dq_tbl_requested_companies) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_tbl_requested_companies
(
	gave_id INT,
	request_info_id INT,
	requested_grade_id INT,
	sp_id INT,
	category_id INT,
	est_gave_date TIMESTAMP,
	gave_date TIMESTAMP,
	original_requested_id INT,
	gave_count SMALLINT,
	member_id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_tbl_requested_companies';
