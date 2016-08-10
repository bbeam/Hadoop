--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_product). 
--*/

--  Creating a dq hive table(dq_product) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_product
(
  product_id INT,
  product_name VARCHAR(100),
  product_type_id INT,
  active_ind TINYINT,
  comments STRING,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by VARCHAR(50),
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  update_by VARCHAR(50),
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_product';
