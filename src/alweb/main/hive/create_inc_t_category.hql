--/*
--  HIVE SCRIPT  : create_inc_t_category.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_category). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_category.hql \
--					--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a incoming hive table(inc_t_category) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_category
(
	category_id STRING,
	name STRING,
	status STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_category';