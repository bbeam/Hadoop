--/*
--  HIVE SCRIPT  : create_dq_report.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_report). 
--*/

--  Creating a DQ hive table(inc_report) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_report
(
	report_id INT,
	member_id INT,
	est_report_date TIMESTAMP,
	report_date TIMESTAMP,
	est_work_completed_date TIMESTAMP,
	work_completed_date TIMESTAMP,
	report_ip_address BIGINT,
	cost DECIMAL(18, 2),
	work_not_done TINYINT,
	hire_again TINYINT,
	employee_id INT,
	grade_exclude_type_id INT,
	report_origin_type_id INT,
	report_status_id INT,
	member_nominated_poh TINYINT,
	page_of_happiness_eligibility_id INT,
	home_build_year STRING,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by STRING,
	report_completeness_id INT,
	est_modified_date TIMESTAMP,
	modified_date TIMESTAMP,
	location_privacy TINYINT,
	do_not_share_with_our_block TINYINT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_report';
