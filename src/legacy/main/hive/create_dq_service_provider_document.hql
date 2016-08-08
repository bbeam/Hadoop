--/*
--  HIVE SCRIPT  : create_dq_service_provider_document.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_service_provider_document). 

--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_service_provider_document.hql \
-- --hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
-- --hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
-- --hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_service_provider_document) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_service_provider_document
(
service_provider_document_id INT,
file_display_name STRING,
file_name STRING,
file_path STRING,
document_type_id INT,
sp_id INT,
user_document_type STRING,
start_date TIMESTAMP,
end_date TIMESTAMP,
approval_status_id INT,
process_date TIMESTAMP,
load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_service_provider_document';