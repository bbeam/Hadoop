--/*
--  HIVE SCRIPT  : create_dq_category_group.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_category_group). 
--*/

--  Creating a DQ hive table(inc_category_group) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_category_group
(
	category_group STRING,
	category_group_description STRING,
	category_group_id INT,
	category_group_is_active TINYINT,
	category_group_type_id INT,
	display_order INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_category_group';
