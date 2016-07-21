--/*
--  HIVE SCRIPT  : create_dq_t_skuitem.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(t_skuitem). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_WORK_DB};

--  Creating a DQ hive table(DQ_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_WORK_DB}.dq_t_skuitem
(
	SkuId INT, 
	MemberPrice DECIMAL(10,2)
)
STORED AS ORC
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/dq_t_skuitem';