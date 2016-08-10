--/*
--  HIVE SCRIPT  : create_dq_t_sku.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(dq_t_sku). 
--*/

--  Creating a DQ hive table(DQ_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_sku
(	
	SkuId INT, 
	Title STRING, 
	IsEmailPromotable TINYINT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_sku';