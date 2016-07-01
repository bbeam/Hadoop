--/*
--  HIVE SCRIPT  : Create_ERR_DQ_t_Sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(ERR_DQ_t_Sku) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(T_SKU_SERDE) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.${hivevar:TABLE_ERR_DQ_T_SKU}
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
	UpdateBy STRING,
	error_code INT,
	error_desc STRING
)
PARTITIONED BY (LoadDate STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:ALWEB_OPERATIONS_DB}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/${hivevar:TABLE_ERR_DQ_T_SKU}';