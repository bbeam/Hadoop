--/*
--  HIVE SCRIPT  : create_dq_category_group_type.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 08, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_category_group_type).
--*/

--  Creating a DQ hive table(inc_category_group_type) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_category_group_type
(
	category_group_type_id INT,
	category_group_type STRING,
	category_group_type_description STRING,
	category_group_type_active TINYINT,
	category_group_type_name STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_category_group_type';
