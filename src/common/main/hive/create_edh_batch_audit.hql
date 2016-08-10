--/*
--  HIVE SCRIPT  : create_edh_batch_audit.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 08, 2016
--  DESCRIPTION  : Creation of Audit hive table. 
--*/

--  Creating a incoming hive table(EDH_BATCH_AUDIT) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.edh_batch_audit
(edh_bus_date STRING,
entity STRING,
table_name STRING,
process STRING,
type STRING,
sub_type STRING,
record_count STRING,
est_time_stamp STRING,
time_stamp STRING,
user_name STRING) 
PARTITIONED BY (edh_bus_month STRING)
STORED AS ORC 
LOCATION '${hivevar:S3_BUCKET}/data/operations/common/edh_batch_audit';
