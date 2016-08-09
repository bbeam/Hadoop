--/*
--  HIVE SCRIPT  : create_dq_exclude_test_sp_ids.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Reports.ExcludeTestSPIDs). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_exclude_test_sp_ids
(
  sp_id INT,
  date_added TIMESTAMP,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/reports/dbo/full/daily/dq_exclude_test_sp_ids';
