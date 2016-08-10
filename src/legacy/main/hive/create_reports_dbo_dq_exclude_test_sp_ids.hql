--/*
--  HIVE SCRIPT  : create_reports_dbo_dq_exclude_test_sp_ids.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Reports.ExcludeTestSPIDs). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_exclude_test_sp_ids
(
  sp_id INT,
  est_date_added TIMESTAMP,
  date_added TIMESTAMP,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/reports/dbo/full/daily/dq_exclude_test_sp_ids';
