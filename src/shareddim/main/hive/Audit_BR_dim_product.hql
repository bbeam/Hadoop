--/*
--  HIVE SCRIPT  : Audit_BR_dim_product.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 11, 2016
--  DESCRIPTION  : Loading data into table (EDH_BATCH_AUDIT) . 
--*/

-- Insert query for loading data into table (EDH_BATCH_AUDIT) with current month partition

INSERT INTO TABLE ${COMMON_OPERATIONS_DB}.${TABLE_EDH_BATCH_AUDIT}
PARTITION(Load_Month)
SELECT '${DATE}' AS Load_Date,
'alweb' AS Entity,
'${WORK_DB}.${TABLE_BR_DIM_PRODUCT}' AS Table_Name,
'DataQuality' AS Process,
'NULL' AS Type ,
'NULL'_desc AS Sub_Type ,
count(*) AS Record_Count,
from_unixtime(unix_timestamp()) AS Time_Stamp,
'AL-EDH' AS User_Name
concat(substr(from_unixtime(unix_timestamp()),0,4),substr(from_unixtime(unix_timestamp()),6,2)) AS Load_Month
 FROM ${WORK_DB}.${TABLE_BR_DIM_PRODUCT}