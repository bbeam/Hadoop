--/*
--  HIVE SCRIPT  : create_dq_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_product). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_product.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_product) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_product
(
  product_id INT,
  product_name VARCHAR(100),
  product_type_id INT,
  active_ind TINYINT,
  comments STRING,
  create_date TIMESTAMP,
  create_by VARCHAR(50),
  update_date TIMESTAMP,
  update_by VARCHAR(50),
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_product';