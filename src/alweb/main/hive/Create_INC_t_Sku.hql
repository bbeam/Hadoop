--/*
--  HIVE SCRIPT  : Create_INC_t_Sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(t_Sku). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB};

--  Creating a incoming hive table(INC_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.${hivevar:TABLE_INC_T_SKU}
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
PARTITIONED BY (LoadDate STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
LOCATION '${hivevar:S3_LOCATION_INCOMING_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:ALWEB_INCOMING_DB}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/${hivevar:TABLE_INC_T_SKU}';