--/*
--  HIVE SCRIPT  : create_inc_exclude_test_sp_ids.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(Reports.ExcludeTestSPIDs). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_exclude_test_sp_ids.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_exclude_test_sp_ids
(
  sp_id STRING,
  date_added STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/reports/full/daily/inc_exclude_test_sp_ids';