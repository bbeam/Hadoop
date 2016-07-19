--/*
--  HIVE SCRIPT  : create_err_tf_dim_product.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive transformation table(err_tf_dim_product). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB};

--  Creating a incoming hive table(TF_dim_product) over the transformaed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_OPERATIONS_DB}.err_tf_dim_product
(
	t_sku_skuid INT,
	t_sku_alid INT,
	t_sku_contractid INT,
	t_sku_title STRING,
	t_sku_description STRING,
	t_sku_termsandconditions STRING,
	t_sku_status STRING,
	t_sku_skutype STRING,
	t_sku_startdatetime TIMESTAMP,
	t_sku_enddatetime TIMESTAMP,
	t_sku_minquantity INT,
	t_sku_maxquantity INT,
	t_sku_maxpurchasequantity INT,
	t_sku_rapidconnect INT,
	t_sku_isautorenew INT,
	t_sku_productid INT,
	t_sku_version INT,
	t_sku_placement STRING,
	t_sku_isemailpromotable INT,
	t_sku_createdate TIMESTAMP,
	t_sku_createby INT,
	t_sku_updatedate TIMESTAMP,
	t_sku_updateby INT,
	t_skuitem_skuitemid INT,
	t_skuitem_contextentityid INT,
	t_skuitem_entitytype STRING,
	t_skuitem_skuid INT,
	t_skuitem_originalprice DECIMAL(10,2),
	t_skuitem_nonmemberprice DECIMAL(10,2),
	t_skuitem_memberprice DECIMAL(10,2),
	t_skuitem_version INT,
	t_skuitem_createdate TIMESTAMP,
	t_skuitem_createby INT,
	t_skuitem_updatedate TIMESTAMP,
	t_skuitem_updateby INT,
	error_type STRING,
	error_desc STRING,
	TFTimeStamp STRING
)
PARTITIONED BY (bus_date STRING)
LOCATION '${hivevar:S3_LOCATION_OPERATIONS_DATA}/${hivevar:SUBJECT_ALWEBMETRICS}/err_tf_dim_product';