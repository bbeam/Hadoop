--/*
--  HIVE SCRIPT  : create_err_dq_t_sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of partitioned hive error table(err_dq_t_skuitem) for DQ layer. 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(T_SKU_SERDE) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.err_dq_t_skuitem
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
PARTITIONED BY (bus_date STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/err_dq_t_skuitem';