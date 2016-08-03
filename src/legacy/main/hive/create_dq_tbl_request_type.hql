--/*
--  HIVE SCRIPT  : create_dq_tbl_requested_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requested_type). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_tbl_requested_type.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}" 
--*/

--  Creating a dq hive table(dq_tbl_requested_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_tbl_request_type
(
	request_type STRING,
	request_type_description STRING ,
	request_type_id SMALLINT ,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_tbl_request_type';