SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_tf_dim_market.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 11, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.${hivevar:AUDIT_TABLE_NAME}
PARTITION(edh_bus_month)
SELECT       edh_bus_date AS edh_bus_date,
            '${hivevar:ENTITY_NAME}' AS entity,
            '${hivevar:TF_DB}.${hivevar:TF_TABLE}' AS table_name,
            'Transformation' AS process,
            'Transformed Records' AS type,
            'Total count' AS sub_type,
            count(*) AS record_count,
            FROM_UTC_TIMESTAMP(unix_timestamp()*1000, 'EST') AS est_time_stamp,
            from_unixtime(unix_timestamp()) AS time_stamp,
            '${hivevar:USER_NAME}' AS user_name,
            CONCAT(SUBSTR((edh_bus_date),0,4),SUBSTR((edh_bus_date),6,2)) AS edh_bus_month
 FROM ${hivevar:TF_DB}.${hivevar:TF_TABLE}  
