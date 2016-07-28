--/*
--  HIVE SCRIPT  : create_dq_t_serviceprovider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 27, 2016
--  DESCRIPTION  : Creation of hive incoming DQtable(dq_t_serviceprovider). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_WORK_DB};

--  Creating a DQ hive table(dq_t_serviceprovider) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_WORK_DB}.dq_t_serviceprovider
(	
	ServiceProviderId BIGINT, 
	ListingId BIGINT, 
	AlId BIGINT, 
	Name VARCHAR(254), 
	Hours VARCHAR(254), 
	JoinedDate TIMESTAMP, 
	Description STRING, 
	ServicesOffered STRING, 
	ServiceProviderType VARCHAR(254), 
	InBusinessSince INT, 
	IsNational TINYINT, 
	Status VARCHAR(254), 
	Version BIGINT, 
	CreateDate TIMESTAMP, 
	CreateBy BIGINT, 
	UpdateDate TIMESTAMP, 
	UpdateBy BIGINT
)
STORED AS ORC
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/dq_t_serviceprovider';  