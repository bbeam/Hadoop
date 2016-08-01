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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "\u0001",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_postal_address';