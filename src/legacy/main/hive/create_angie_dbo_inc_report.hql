--/*
--  HIVE SCRIPT  : create_inc_report.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 09, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_report). 
--*/

--  Creating a incoming hive table(inc_report) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_report
(
	report_id STRING,
	member_id STRING,
	report_date STRING,
	work_completed_date STRING,
	report_ip_address STRING,
	cost STRING,
	work_not_done STRING,
	hire_again STRING,
	employee_id STRING,
	grade_exclude_type_id STRING,
	report_origin_type_id STRING,
	report_status_id STRING,
	member_nominated_poh STRING,
	page_of_happiness_eligibility_id STRING,
	home_build_year STRING,
	create_date STRING,
	create_by STRING,
	report_completeness_id STRING,
	modified_date STRING,
	location_privacy STRING,
	do_not_share_with_our_block STRING


	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/full/daily/inc_report';