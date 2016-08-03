--/*
--  HIVE SCRIPT  : create_inc_tbl_requested_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requested_type). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_tbl_requested_type.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_tbl_requested_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_tbl_requested_type
(
	request_type STRING,
	request_type_description STRING,
	request_type_id STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_tbl_requested_type';