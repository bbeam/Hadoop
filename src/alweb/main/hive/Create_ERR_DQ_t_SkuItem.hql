--/*
--  HIVE SCRIPT  : Create_ERR_DQ_t_Sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(ERR_DQ_t_SkuItem) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(T_SKU_SERDE) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.${hivevar:TABLE_ERR_DQ_T_SKUITEM}
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
	UpdateBy STRING,
	error_type STRING,
	error_desc STRING,
	DQTimeStamp STRING
)
PARTITIONED BY (LoadDate STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/${hivevar:TABLE_ERR_DQ_T_SKUITEM}';