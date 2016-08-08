--/*
--  HIVE SCRIPT  : create_dq_category_group_type.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_category_group_type). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_dq_category_group_type.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}"
--*/

--  Creating a DQ hive table(inc_category_group_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_category_group_type
(
	category_group_type_id INT,
	category_group_type STRING,
	category_group_type_description STRING,
	category_group_type_active TINYINT,
	category_group_type_name STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_category_group_type';