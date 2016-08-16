--/*
--HIVE SCRIPT: create_angieslist_dq_t_sku.hql
--AUTHOR : Varun Rauthan
--DATE : Jun 23, 2016
--DESCRIPTION: Creation of hive incoming table(dq_t_sku). 
--*/

--Creating a DQ hive table(DQ_t_Sku) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_sku
(	
	sku_id INT,
	al_id INT,
	contract_id INT,
	title VARCHAR(254),
	description STRING,
	terms_and_conditions STRING,
	status VARCHAR(254),
	sku_type VARCHAR(254),
	est_start_datetime TIMESTAMP,
	start_datetime TIMESTAMP,
	est_end_datetime TIMESTAMP,
	end_datetime TIMESTAMP,
	min_quantity INT,
	max_quantity INT,
	max_purchase_quantity INT,
	rapid_connect TINYINT,
	is_auto_renew TINYINT,
	product_id INT,
	version INT,
	placement VARCHAR(32),
	is_email_promotable TINYINT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_sku';