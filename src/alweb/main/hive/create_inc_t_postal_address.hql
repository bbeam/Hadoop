--/*
--  HIVE SCRIPT  : create_inc_t_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_postal_address). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_postal_address.hql \
--					--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a incoming hive table(inc_t_postal_address) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_postal_address
(
	SkuId STRING, 
	AlId STRING, 
	ContractId STRING, 
	Title STRING, 
	Description STRING, 
	TermsAndConditions STRING, 
	Status STRING, 
	SkuType STRING, 
	StartDateTime STRING, 
	EndDateTime STRING, 
	MinQuantity STRING, 
	MaxQuantity STRING, 
	MaxPurchaseQuantity STRING, 
	RapidConnect STRING, 
	IsAutoRenew STRING, 
	ProductId STRING, 
	Version STRING, 
	Placement STRING, 
	IsEmailPromotable STRING, 
	CreateDate STRING, 
	CreateBy STRING, 
	UpdateDate STRING, 
	UpdateBy STRING
)
PARTITIONED BY (bus_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "\u0001",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_postal_addresss';