--/*
--  HIVE SCRIPT  : create_dq_t_sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(dq_t_sku). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a DQ hive table(DQ_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.dq_t_sku
(	
	SkuId INT, 
	Title STRING, 
	IsEmailPromotable INT, 
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:WORK_DB}/dq_t_sku';  