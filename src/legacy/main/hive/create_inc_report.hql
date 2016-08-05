--/*
--  HIVE SCRIPT  : create_inc_report.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 09, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_report). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_report.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_report) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_report
(
	cost STRING,
	create_by STRING,
	create_date STRING,
	do_not_share_with_our_block STRING,
	employee_id STRING,
	grade_exclude_type_id STRING,
	hire_again STRING,                  
	home_build_year STRING,                
	location_privacy STRING,
	member_id STRING,
	member_nominated_poh STRING,
	modified_date STRING,
	page_of_happiness_eligibility_id STRING,
	report_completeness_id STRING,
	report_date STRING,
	report_id STRING,     
	report_ip_address STRING,         
	report_origin_type_id STRING,
	report_status_id STRING,
	work_completed_date STRING, 
	work_not_done STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_report';