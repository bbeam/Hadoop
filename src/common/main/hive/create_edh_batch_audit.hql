--/*
--  HIVE SCRIPT  : create_edh_batch_audit.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 08, 2016
--  DESCRIPTION  : Creation of Audit hive table. 
--*/

--  Creating a incoming hive table(EDH_BATCH_AUDIT) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:COMMON_OPERATIONS_DB}.edh_batch_audit
(bus_date STRING,
entity STRING,
table_name STRING,
process STRING,
type STRING,
sub_type STRING,
record_count STRING,
time_stamp STRING,
user_name STRING) 
PARTITIONED BY (bus_Month STRING)
STORED AS ORC 
LOCATION '${hivevar:S3_BUCKET}/${hivevar:S3_LOCATION_OPERATIONS_DATA}/common/edh_batch_audit';
