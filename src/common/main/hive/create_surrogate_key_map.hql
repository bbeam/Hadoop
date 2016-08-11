--/*
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 25, 2016
--  DESCRIPTION  : The script is used for surrogate key generation. 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map
(
max_sk int
)
PARTITIONED BY (table_name String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://al-edh-dev/data/operations/common/surrogate_key_map';
