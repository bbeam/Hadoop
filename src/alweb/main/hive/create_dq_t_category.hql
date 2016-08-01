--/*
--  HIVE SCRIPT  : create_dq_t_category.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_t_category). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_t_category.hql \
--					--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a DQ hive table(inc_t_category) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_category
(
	category_id INT,
	name VARCHAR(254),
	status VARCHAR(254),
	create_date TIMESTAMP,
	create_by INT,
	update_date TIMESTAMP,
	update_by INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_category';