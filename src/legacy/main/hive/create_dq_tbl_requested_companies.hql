--/*
--  HIVE SCRIPT  : create_dq_tbl_requested_companies.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_tbl_requested_companies). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_tbl_requested_companies.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}" 


--*/

--  Creating a dq hive table(dq_tbl_requested_companies) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_tbl_requested_companies
(
	gave_id INT,
	request_info_id INT,
	requested_grade_id INT,
	sp_id INT,
	category_id INT,
	gave_date TIMESTAMP,
	original_requested_id INT,
	gave_count SMALLINT,
	member_id INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_tbl_requested_companies';