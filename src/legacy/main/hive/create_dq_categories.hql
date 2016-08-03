--/*
--  HIVE SCRIPT  : create_dq_categories.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_categories). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_dq_categories.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}"
--*/

--  Creating a DQ hive table(inc_categories) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_categories
(
	category_id INT,
	category_name STRING,
	category STRING,
	fundraiser_category_id INT,
	rank INT, 
	category_group_id INT, 
	category_subgroup_id INT, 
	seo_category_id INT, 
	average_job_cost DECIMAL(18,2), 
	is_active TINYINT, 
	category_travel_direction_id INT, 
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_categories';