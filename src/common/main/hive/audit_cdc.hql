SET hive.exec.dynamic.partition.mode=non-strict;
--/*
--  HIVE SCRIPT  : audit_tf_dim_market.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.${hivevar:AUDIT_TABLE_NAME}
PARTITION(edh_bus_date,table_name)
SELECT      '${hivevar:ENTITY_NAME}' AS entity,
            'CDC' AS process,
             CASE action_cd
            WHEN 'I' THEN 'Insert'
            WHEN 'U' THEN 'Update'
            WHEN 'NCD' THEN 'No Change or Delete'
            ELSE 'Unknown'
            END AS type,
			'Record Count' AS sub_type,
            count(*) AS record_count,
            FROM_UTC_TIMESTAMP(unix_timestamp()*1000, 'EST') AS est_time_stamp,
            from_unixtime(unix_timestamp()) AS time_stamp,
            '${hivevar:USER_NAME}' AS user_name,
            '${hivevar:EDH_BUS_DATE}' AS edh_bus_date,
            '${hivevar:WORK_CDC_DB}.${hivevar:WORK_CDC_TABLE}' AS table_name
 FROM ${hivevar:WORK_CDC_DB}.${hivevar:WORK_CDC_TABLE}
 GROUP BY action_cd;
  