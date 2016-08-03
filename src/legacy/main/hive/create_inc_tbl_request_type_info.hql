--/*
--  HIVE SCRIPT  : create_inc_tbl_requested_type_info.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requested_type_info). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_tbl_requested_type_info.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_tbl_requested_type_info) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_tbl_requested_type_info
(
	bln_declined_alarm STRING,
	cat_id STRING,
	key_word STRING,
	request_count STRING,
	request_id STRING,
	request_info_id STRING,
	request_type_id STRING,
	sp_id STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_tbl_requested_type_info';