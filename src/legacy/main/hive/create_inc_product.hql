--/*
--  HIVE SCRIPT  : create_inc_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_product). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_product.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_product) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_product
(
	product_id STRING,
	product_name STRING,
	product_type_id STRING,
	active_ind STRING,
	comments STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY(edh_bus_date STRING) 
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_product';