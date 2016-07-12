--/*
--  HIVE SCRIPT  : Create_INC_t_SkuItem.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(t_SkuItem).
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB};

--  Creating a incoming hive table(INC_t_SkuItem) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.${hivevar:TABLE_INC_T_SKUITEM}
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
PARTITIONED BY (LoadDate STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
LOCATION '${hivevar:S3_LOCATION_INCOMING_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/${hivevar:TABLE_INC_T_SKUITEM}';