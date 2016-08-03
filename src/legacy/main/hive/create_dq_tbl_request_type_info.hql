--/*
--  HIVE SCRIPT  : create_dq_tbl_requested_type_info.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requested_type_info). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_tbl_requested_type_info.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}" 
--*/

--  Creating a dq hive table(dq_tbl_requested_type_info) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_tbl_request_type_info
(
	bln_declined_alarm SMALLINT,
	cat_id INT,
	key_word STRING,
	request_count SMALLINT,
	request_id INT,
	request_info_id INT,
	request_type_id SMALLINT ,
	sp_id INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_tbl_request_type_info';