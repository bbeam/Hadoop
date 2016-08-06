--/*
--  HIVE SCRIPT  : create_dq_report.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_report). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/legacy/main/hive/create_dq_report.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}"
--*/

--  Creating a DQ hive table(inc_report) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_report
(
	cost DECIMAL(18, 2),
	create_by STRING,
	create_date TIMESTAMP,
	do_not_share_with_our_block TINYINT,
	employee_id INT,
	grade_exclude_type_id INT,
	hire_again TINYINT,                  
	home_build_year STRING,                
	location_privacy TINYINT,
	member_id INT,
	member_nominated_poh TINYINT,
	modified_date TIMESTAMP,
	page_of_happiness_eligibility_id INT,
	report_completeness_id INT,
	report_date TIMESTAMP,
	report_id INT,     
	report_ip_address BIGINT,         
	report_origin_type_id INT,
	report_status_id INT,
	work_completed_date TIMESTAMP, 
	work_not_done	TINYINT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_report';