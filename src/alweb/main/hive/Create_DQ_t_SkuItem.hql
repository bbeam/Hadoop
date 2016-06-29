--/*
--  HIVE SCRIPT  : Create_DQ_t_SkuItem.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(t_Sku). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a DQ hive table(DQ_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_DQ_T_SKUITEM}
(
	SkuItemId INT, 
	ContextEntityId INT, 
	EntityType STRING, 
	SkuId INT, 
	OriginalPrice DECIMAL(10,2), 
	NonMemberPrice DECIMAL(10,2), 
	MemberPrice DECIMAL(10,2), 
	Version INT, 
	CreateDate TIMESTAMP, 
	CreateBy INT, 	
	UpdateDate TIMESTAMP, 
	UpdateBy INT
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:WORK_DB}/${hivevar:TABLE_DQ_T_SKUITEM}';