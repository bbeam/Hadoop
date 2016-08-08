--/*
--  HIVE SCRIPT  : create_inc_service_provider_document.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_service_provider_document). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_service_provider_document.hql \
-- --hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
-- --hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
-- --hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_service_provider_document) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_service_provider_document
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_service_provider_document';