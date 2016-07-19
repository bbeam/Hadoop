--/*
--  HIVE SCRIPT  : audit_dq_t_skuitem.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 11, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${COMMON_OPERATIONS_DB}.edh_batch_audit
PARTITION(bus_month)
SELECT bus_date AS load_date,
'alweb' AS entity,
'${WORK_DB}.dq_t_skuitem' AS table_name,
'DataQuality' AS process,
'NULL' AS type ,
'NULL' AS sub_type ,
count(*) AS Record_Count,
from_unixtime(unix_timestamp()) AS time_stamp,
'AL-EDH' AS user_name
concat(substr(from_unixtime(unix_timestamp()),0,4),substr(from_unixtime(unix_timestamp()),6,2)) AS bus_month
 FROM ${WORK_DB}.dq_t_skuitem  GROUP BY bus_date