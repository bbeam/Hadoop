--/*
--  HIVE SCRIPT  : create_inc_mbr_pay_status.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.mbr_pay_status) 
--  Execute command:
-- 	hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_mbr_pay_status.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_mbr_pay_status
(
	member_id STRING,
	pay_status STRING,
	status_date STRING,
	id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/reports/full/daily/inc_mbr_pay_status';
