SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_tf_fact_multi_event_source.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.${hivevar:AUDIT_TABLE_NAME}
PARTITION(edh_bus_date,table_name)
SELECT      '${hivevar:ENTITY_NAME}' AS entity,
             CONCAT(event_type,'_',event_type_key,' Transformation') AS process,
            'Transformed Records' AS type,
            'Total count' AS sub_type,
            count(*) AS record_count,
            FROM_UTC_TIMESTAMP(unix_timestamp()*1000, 'EST') AS est_time_stamp,
            from_unixtime(unix_timestamp()) AS time_stamp,
            '${hivevar:USER_NAME}' AS user_name,
            '${hivevar:EDH_BUS_DATE}' AS edh_bus_date,
            '${hivevar:TF_DB}.tf_fact_web_metrics' AS table_name
 FROM ${hivevar:TF_DB}.tf_nk_fact_web_metrics 
 WHERE event_type_key in (${hivevar:TF_EVENT_KEY})
 GROUP BY event_type,event_type_key