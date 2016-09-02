--/*
--HIVE SCRIPT: create_angieslist_inc_t_sku.hql
--AUTHOR : Varun Rauthan
--DATE : Jun 23, 2016
--DESCRIPTION: Creation of hive incoming table(inc_t_sku). 
--*/

--Creating a incoming hive table(INC_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_sku
(
	sku_id STRING,
	al_id STRING,
	contract_id STRING,
	title STRING,
	description STRING,
	terms_and_conditions STRING,
	status STRING,
	sku_type STRING,
	start_datetime STRING,
	end_datetime STRING,
	min_quantity STRING,
	max_quantity STRING,
	max_purchase_quantity STRING,
	rapid_connect STRING,
	is_auto_renew STRING,
	product_id STRING,
	version STRING,
	placement STRING,
	is_email_promotable STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_sku';