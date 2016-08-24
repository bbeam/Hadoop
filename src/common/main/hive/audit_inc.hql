SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_inc.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 11, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.edh_batch_audit
PARTITION(edh_bus_date,table_name)
SELECT
		'${hivevar:ENTITY_NAME}' AS entity,
		'Incoming' AS process,
		'Incoming Records' AS type,
		'Total count' AS sub_type,
		count(*) AS record_count,
		FROM_UTC_TIMESTAMP(unix_timestamp()*1000, 'EST') AS est_time_stamp,
		from_unixtime(unix_timestamp()) AS time_stamp,
		'${hivevar:USER_NAME}' AS user_name,
		edh_bus_date AS edh_bus_date,
		'${hivevar:INCOMING_DB}.${hivevar:INCOMING_TABLE}' AS table_name
 FROM ${hivevar:INCOMING_DB}.${hivevar:INCOMING_TABLE}  
 WHERE edh_bus_date='${hivevar:EDH_BUS_DATE}' GROUP BY edh_bus_date;