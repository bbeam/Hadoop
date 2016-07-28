--/*
--  HIVE SCRIPT  : create_inc_t_serviceprovider.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : July 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_serviceprovider). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB};

--  Creating a incoming hive table(inc_t_serviceprovider) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_serviceprovider
(
	ServiceProviderId STRING, 
	ListingId STRING, 
	AlId STRING, 
	Name STRING, 
	Hours STRING, 
	JoinedDate STRING, 
	Description STRING, 
	ServicesOffered STRING, 
	ServiceProviderType STRING, 
	InBusinessSince STRING, 
	IsNational STRING, 
	Status STRING, 
	Version STRING, 
	CreateDate STRING, 
	CreateBy STRING, 
	UpdateDate STRING, 
	UpdateBy STRING
)
PARTITIONED BY (bus_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
LOCATION '${hivevar:S3_LOCATION_INCOMING_DATA}/${hivevar:SOURCE_ALWEB}/${hivevar:SOURCE_SCHEMA}/${hivevar:EXTRACTIONTYPE_FULL}/${hivevar:FREQUENCY_DAILY}/inc_t_serviceprovider';



