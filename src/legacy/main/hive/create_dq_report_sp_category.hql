--/*
--  HIVE SCRIPT  : create_dq_report_sp_category.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_report_sp_category). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_report_sp_category.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_report_sp_category) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_report_sp_category
(
	category_id INT,
	create_by VARCHAR(50),
	create_date TIMESTAMP,
	is_primary TINYINT,
	report_id INT,
	report_sp_category_id INT,
	sp_id INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_report_sp_category';