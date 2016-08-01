--/*
--  HIVE SCRIPT  : create_inc_t_skuitem.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_skuitem).
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_skuitem.hql \
--					--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a incoming hive table(INC_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_skuitem
(
	SkuItemId STRING, 
	ContextEntityId STRING, 
	EntityType STRING, 
	SkuId STRING, 
	OriginalPrice STRING, 
	NonMemberPrice STRING, 
	MemberPrice STRING, 
	Version STRING, 
	CreateDate STRING, 
	CreateBy STRING, 	
	UpdateDate STRING, 
	UpdateBy STRING
)
PARTITIONED BY (edh_bus_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "\u0001",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_skuitem';