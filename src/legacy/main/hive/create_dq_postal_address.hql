--/*
--  HIVE SCRIPT  : create_dq_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.PostalAddress)
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_postal_address
(
	postal_address_id INT,
	postal_format_id INT,
	address1 STRING,
	address2 STRING,
	city STRING,
	state STRING,
	postal_code STRING,
	country_code_id INT,
	latitude DECIMAL(19,9),
	longitude DECIMAL(19,9),
	coordinate_confidence INT,
	est_process_date TIMESTAMP,
	process_date TIMESTAMP,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_postal_address';
