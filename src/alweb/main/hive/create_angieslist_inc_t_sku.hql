--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_sku). 
--*/

--  Creating a incoming hive table(INC_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_sku
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
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_sku';