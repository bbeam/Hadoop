--/*
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 25, 2016
--  DESCRIPTION  : The script is used for surrogate key generation. 
--*/

--  Drop the table if already exists
DROP TABLE IF EXISTS common_operations.surrogate_key_map;

CREATE EXTERNAL TABLE IF NOT EXISTS ops_common.surrogate_key_map
(
max_sk int
)
PARTITIONED BY (table_name String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://al-edh-dev/data/operations/common/surrogate_key_map';
