--/*
--  HIVE SCRIPT  : create_reports_dbo_inc_exclude_test_sp_ids.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(Reports.ExcludeTestSPIDs). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_exclude_test_sp_ids
(
  sp_id STRING,
  date_added STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/reports/dbo/full/daily/inc_exclude_test_sp_ids';