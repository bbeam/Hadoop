--/*
--  AUTHOR       : Abhijeet Mehar
--  DATE         : Jun 25, 2016
--  DESCRIPTION  : The script is used for surrogate key generation. 
--*/

--  Drop the table if already exists
DROP TABLE IF EXISTS alwebmetrics_Gold.SK_map;

CREATE EXTERNAL TABLE alwebmetrics_Gold.SK_map
(
max_sk int
)
PARTITIONED BY (table_name String) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
LOCATION 's3://al-edh-dm/data/operations/common'; 


/* Turning off partition mode to non strict to allow dynamic partition without a single static partition. */
SET hive.exec.dynamic.partition.mode=non-strict;


/* Adding a record for the first run */
INSERT OVERWRITE TABLE alwebmetrics_Gold.sk_map 
	PARTITION(table_name) 
	select 1, 'dim_product' FROM work.dq_t_sku LIMIT 1;


