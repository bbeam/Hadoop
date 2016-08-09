--/*
--  HIVE SCRIPT  : create_dq_t_skuitem.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(t_skuitem). 
--*/

--  Creating a DQ hive table(DQ_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_sku_item
(
  sku_item_id INT,
  context_entity_id INT,
  entity_type VARCHAR(254),
  sku_id INT,
  original_price DECIMAL(10,2),
  non_member_price DECIMAL(10,2),
  member_price DECIMAL(10,2), 
  version INT,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by INT,
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  update_by INT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_sku_item';
