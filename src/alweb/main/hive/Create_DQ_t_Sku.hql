--/*
--  HIVE SCRIPT  : Create_DQ_t_Sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(t_Sku). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a DQ hive table(DQ_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_DQ_T_SKU}
(	
	SkuId INT, 
	AlId INT, 
	ContractId INT, 
	Title STRING, 
	Description STRING, 
	TermsAndConditions STRING, 
	Status STRING, 
	SkuType STRING, 
	StartDateTime TIMESTAMP, 
	EndDateTime TIMESTAMP, 
	MinQuantity INT, 
	MaxQuantity INT, 
	MaxPurchaseQuantity INT, 
	RapidConnect INT, 
	IsAutoRenew INT, 
	ProductId INT, 
	Version INT, 
	Placement STRING, 
	IsEmailPromotable INT, 
	CreateDate TIMESTAMP, 
	CreateBy INT, 
	UpdateDate TIMESTAMP, 
	UpdateBy INT
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:WORK_DB}/${hivevar:TABLE_DQ_T_SKU}';  