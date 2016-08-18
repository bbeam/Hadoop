--/*
--  HIVE SCRIPT  : create_dim_category.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 16, 2016
--  DESCRIPTION  : Creation of hive TF work table work_shared_dim.tf_dim_category 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.tf_dim_category
(
	category_id	INT,
	category STRING ,
	legacy_category STRING,
	new_world_category STRING,
	additional_category_nm STRING,
	is_active BOOLEAN,
	category_group STRING, 
	category_group_type STRING 

);