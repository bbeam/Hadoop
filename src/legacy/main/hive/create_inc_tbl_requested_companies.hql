--/*
--  HIVE SCRIPT  : create_inc_tbl_requested_companies.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_tbl_requested_companies). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_tbl_requested_companies.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_tbl_requested_companies) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_tbl_requested_companies
(
gave_id STRING,
request_info_id STRING,
requested_grade_id STRING,
sp_id STRING,
category_id STRING,
gave_date STRING,
original_requested_id STRING,
gave_count STRING,
member_id STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_tbl_requested_companies';