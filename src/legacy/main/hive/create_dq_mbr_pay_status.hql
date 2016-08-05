--/*
--  HIVE SCRIPT  : create_dq_mbr_pay_status.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.mbr_pay_status) 
--  Execute command:
-- 	hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_mbr_pay_status.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_mbr_pay_status
(
	id INT,
	member_id INT,
	pay_status char(4),
	status_date TIMESTAMP,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/reports/full/daily/dq_mbr_pay_status';
