--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_postal_address). 
--*/

--  Creating a incoming hive table(inc_t_postal_address) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_postal_address
(
	postal_address_id STRING,
	address_type STRING,
	address_first_line STRING,
	address_second_line STRING,
	city_id STRING,
	region_id STRING,
	country_id STRING,
	postal_code STRING,
	year_built STRING,
	longitude STRING,
	latitude STRING,
	advertising_zone STRING,
	validation_status STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING,
	version STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_postal_address';