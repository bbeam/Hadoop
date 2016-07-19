--/*
--  HIVE SCRIPT  : create_err_dq_t_sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(err_dq_t_sku) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(T_SKU_SERDE) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.err_dq_t_sku
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
	error_type STRING,
	error_desc STRING,
	DQTimeStamp STRING
)
PARTITIONED BY (bus_date STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/err_dq_t_sku';