--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_skuitem.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_skuitem).
--*/

--  Creating a incoming hive table(INC_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_skuitem
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_skuitem';