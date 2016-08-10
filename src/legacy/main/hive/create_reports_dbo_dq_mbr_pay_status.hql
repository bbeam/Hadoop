--/*
--  HIVE SCRIPT  : create_reports_dbo_dq_mbr_pay_status.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.mbr_pay_status) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_mbr_pay_status
(
	member_id INT,
	pay_status char(4),
	est_status_date TIMESTAMP,
	status_date TIMESTAMP,
	id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/reports/dbo/full/daily/dq_mbr_pay_status';
