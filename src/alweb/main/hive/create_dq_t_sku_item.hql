--/*
--  HIVE SCRIPT  : create_dq_t_skuitem.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(t_skuitem). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/alweb/main/hive/create_inc_t_sku_item.hql \
						--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
						--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
						--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB};

--  Creating a DQ hive table(DQ_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_sku_item
(
  sku_item_id INT,
  context_entity_id INT,
  entity_type VARCHAR(254),
  sku_id INT,
  original_price DECIMAL(10,2),
  non_member_price DECIMAL(10,2),
  member_price DECIMAL(10,2), 
  version INT,
  create_date TIMESTAMP,
  create_by INT,
  update_date TIMESTAMP,
  update_by INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_sku_item';
