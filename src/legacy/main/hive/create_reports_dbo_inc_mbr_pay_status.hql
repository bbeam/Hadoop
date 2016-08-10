--/*
--  HIVE SCRIPT  : create_inc_mbr_pay_status.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.mbr_pay_status) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_mbr_pay_status
(
	member_id STRING,
	pay_status STRING,
	status_date STRING,
	id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/reports/dbo/full/daily/inc_mbr_pay_status';
