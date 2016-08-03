--/*
--  HIVE SCRIPT  : create_dq_product_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_product_type). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_product_type.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_t_sku) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_product_type
(
	product_type_id INT,
	product_type_name VARCHAR(255),
	product_type_description VARCHAR(255),
	product_type_active TINYINT,
	product_type_is_primary TINYINT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_product_type';