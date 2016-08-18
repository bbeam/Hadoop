SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_tf_dim_market.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.${hivevar:AUDIT_TABLE_NAME}
PARTITION(edh_bus_month)
SELECT      '${hivevar:EDH_BUS_DATE}' AS edh_bus_date,
            '${hivevar:ENTITY_NAME}' AS entity,
            '${hivevar:WORK_CDC_DB}.${hivevar:WORK_CDC_TABLE}' AS table_name,
            'Dimension Load' AS process,
            'Dimension Load in Gold Hive Table' AS type,
            'Total count' AS sub_type,
            count(*) AS record_count,
            FROM_UTC_TIMESTAMP(unix_timestamp()*1000, 'EST') AS est_time_stamp,
            from_unixtime(unix_timestamp()) AS time_stamp,
            '${hivevar:USER_NAME}' AS user_name,
            '${hivevar:EDH_BUS_MONTH}' AS edh_bus_month
 FROM ${hivevar:GOLD_DIM_DB}.${hivevar:GOLD_DIM_TABLE};
