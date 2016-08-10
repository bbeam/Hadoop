--/*
--  HIVE SCRIPT  : create_inc_service_provider_document.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_service_provider_document). 
--*/


--  Creating a incoming hive table(inc_service_provider_document) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_service_provider_document
(
service_provider_document_id STRING,
file_display_name STRING,
file_name STRING,
file_path STRING,
document_type_id STRING,
sp_id STRING,
user_document_type STRING,
start_date STRING,
end_date STRING,
approval_status_id STRING,
process_date STRING
)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_service_provider_document';