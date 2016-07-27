SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_inc.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 11, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE common_operations.edh_batch_audit
PARTITION(bus_month)
SELECT       bus_date AS bus_date,
            '${hivevar:ENTITY_NAME}' AS entity,
            '${hivevar:INCOMING_DB}.${hivevar:INCOMING_TABLE}' AS table_name,
            'Incoming' AS process,
            'Incoming Records' AS type,
            'Total count' AS sub_type,
            count(*) AS record_count,
            FROM(unix_timestamp()) AS time_stamp,
            '${hivevar:USER_NAME}' AS user_name,
            CONCAT(SUBSTR.bus_date),0,4),SUBSTR((bus_date),6,2)) AS bus_month
 FROM ${hivevar:INCOMING_DB}.${hivevar:INCOMING_TABLE}  WHERE bus_date=${hivevar:BUS_DATE};