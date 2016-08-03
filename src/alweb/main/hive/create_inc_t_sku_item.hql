--/*
--  HIVE SCRIPT  : create_inc_t_sku_item.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_sku_item). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/alweb/main/hive/create_inc_t_sku_item.hql \
						--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
						--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
						--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a incoming hive table(inc_t_sku_item) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_sku_item
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\u0001",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_sku_item';
