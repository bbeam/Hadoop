--/*
--  HIVE SCRIPT  : create_dq_categories.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_categories). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_categories.hql \
--					--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a DQ hive table(inc_categories) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_categories
(
	category_id INT,
	category_name STRING,
	category STRING,
	fundraiser_category_id INT,
	rank INT, 
	category_group_id INT, 
	category_subgroup_id INT, 
	seo_category_id INT, 
	average_job_cost DECIMAL, 
	is_active TINYINT, 
	category_travel_direction_id INT, 
	checksum BIGINT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_categories';