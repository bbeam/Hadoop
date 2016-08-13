--/*
--  HIVE SCRIPT  : create_tf_dim_market.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.tf_dim_market 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/shareddim/main/hive/create_tf_dim_market.hql \
-- -hivevar HIVE_DB=$WORK_SHARED_DIM_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET 
--*/

CREATE TABLE IF NOT EXISTS ${hivevar:HIVE_DB}.tf_dim_market
(
	market_id	int,
	market_nm	string
);