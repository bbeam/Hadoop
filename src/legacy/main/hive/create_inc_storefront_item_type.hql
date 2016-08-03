--/*
--  HIVE SCRIPT  : create_inc_storefront_item_type.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_storefront_item_type). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/alweb/main/hive/create_inc_t_sku_item.hql \
						--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
						--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
						--hivevar S3_BUCKET="${S3_BUCKET}"

--*/

--  Creating a incoming hive table(legacy_storefront_item_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_storefront_item_type
(
  storefront_item_type_id STRING,
  storefront_item_type_name STRING,
  web_content_key STRING,
  receipt_email_object STRING,
  instructions_email_object STRING,
  service_provider_email_object STRING,
  create_date STRING,
  create_by STRING,
  storefront_item_type_default_fee STRING,
  product_id STRING,
  checksum STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_storefront_item_type';