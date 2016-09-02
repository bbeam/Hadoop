--/*
--  HIVE SCRIPT  : create_scd_dim_product.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 24, 2016
--  DESCRIPTION  : Creation of hive SCD work table
--*/

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME}
(
	product_key BIGINT,
	source_ak INT,
	source_table STRING,
	source_column STRING,
	master_product_group STRING, 
	product_type STRING, 
	product STRING, 
	unit_price DECIMAL(10,2), 
	source STRING,
	action_cd STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:WORK_DIR}/data/work/shareddim/scd_dim_product';