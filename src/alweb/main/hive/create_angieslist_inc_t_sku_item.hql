--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_sku_item.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_sku_item). 
--*/

--  Creating a incoming hive table(inc_t_sku_item) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_sku_item
(
  sku_item_id STRING, 
  context_entity_id STRING, 
  entity_type STRING, 
  sku_id STRING, 
  original_price STRING, 
  non_member_price STRING, 
  member_price STRING, 
  version STRING, 
  create_date STRING, 
  create_by STRING,   
  update_date STRING, 
  update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_sku_item';
