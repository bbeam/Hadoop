--/*
--  HIVE SCRIPT  : create_inc_t_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_postal_address). 
--*/

--  Creating a incoming hive table(inc_t_postal_address) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_postal_address
(
	postal_address_id INT,
	address_type VARCHAR(254),
	address_first_line VARCHAR(254),
	address_second_line VARCHAR(254),
	city_id INT,
	region_id INT,
	country_id INT,
	postal_code VARCHAR(50),
	year_built INT,
	longitude DOUBLE,
	latitude DOUBLE,
	advertising_zone INT,
	validation_status STRING,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by INT,
	version INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_postal_address';