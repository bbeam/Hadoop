--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.PostalAddress). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_postal_address
(
	postal_address_id STRING,
	postal_format_id STRING,
	address1 STRING,
	address2 STRING,
	city STRING,
	state STRING,
	postal_code STRING,
	country_code_id STRING,
	latitude STRING,
	longitude STRING,
	coordinate_confidence STRING,
	process_date STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_postal_address';