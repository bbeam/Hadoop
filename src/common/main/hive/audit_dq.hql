SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_dq.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 11, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE common_operations.edh_batch_audit
PARTITION(edh_bus_month)
SELECT       '${hivevar:EDH_BUS_DATE}' AS edh_bus_date,
             '${hivevar:ENTITY_NAME}' AS entity,
             '${hivevar:GOLD_DB}.${hivevar:DQ_TABLE}' AS table_name,
             'DataQuality' AS process,
             'Good Records' AS type ,
             'Total Count' AS sub_type ,
             count(*) AS record_count,
             from_unixtime(unix_timestamp()) AS time_stamp,
             '${hivevar:USER_NAME}' AS user_name,
             '${hivevar:EDH_BUS_MONTH}' AS edh_bus_month
 FROM ${GOLD_DB}.${hivevar:DQ_TABLE};
 
INSERT INTO TABLE common_operations.edh_batch_audit
PARTITION(edh_bus_month)
SELECT       '${hivevar:EDH_BUS_DATE}' AS edh_bus_date,
             entity,
             table_name,
             'DataQuality' AS process,
             error_type AS type ,
             error_desc AS sub_type ,
             count(*) AS record_count,
             from_unixtime(unix_timestamp()) AS time_stamp,
             '${hivevar:USER_NAME}' AS user_name,
             '${hivevar:EDH_BUS_MONTH}' AS edh_bus_month
 FROM common_operations.edh_batch_error 
 WHERE table_name='${hivevar:INCOMING_DB}.${hivevar:INCOMING_TABLE}' 
       AND edh_bus_date = '${hivevar:EDH_BUS_DATE}'
 GROUP BY error_type, error_desc,entity,table_name
 HAVING count(*) > 0;
 
 
 