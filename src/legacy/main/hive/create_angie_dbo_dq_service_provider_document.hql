--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_service_provider_document.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_service_provider_document).
--*/

--  Creating a dq hive table(dq_service_provider_document) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_service_provider_document
(
service_provider_document_id INT,
file_display_name STRING,
file_name STRING,
file_path STRING,
document_type_id INT,
sp_id INT,
user_document_type STRING,
est_start_date TIMESTAMP,
start_date TIMESTAMP,
end_date TIMESTAMP,
approval_status_id INT,
est_process_date TIMESTAMP,
process_date TIMESTAMP,
est_load_timestamp TIMESTAMP,
load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_service_provider_document';
