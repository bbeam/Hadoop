--/*
--  HIVE SCRIPT  : create_inc_t_postal_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_postal_address). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_inc_t_postal_address.hql \
--					--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a incoming hive table(inc_t_postal_address) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_postal_address
(
	postal_address_id INT,
	address_type VARCHAR(254),
	address_first_line VARCHAR(254),
	address_second_line VARCHAR(254),0
	city_id INT,
	region_id INT,
	country_id INT,
	postal_code VARCHAR(50),
	year_built INT(4),0
	longitude DOUBLE,0
	latitude DOUBLE,0
	advertising_zone INT,0
	validation_status STRING,
	create_date TIMESTAMP,
	create_by INT,
	update_date TIMESTAMP,
	update_by INT,
	version INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_postal_addresss';