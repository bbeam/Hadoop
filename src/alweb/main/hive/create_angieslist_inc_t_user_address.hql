--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_user_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_user_address). 
--*/ 

--  Creating a incoming hive table(inc_t_user_address) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_user_address
(
	user_address_id STRING,
	postal_address_id STRING,
	user_id STRING,
	is_primary STRING,
	al_id STRING,
	location_description STRING,
	version STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_user_address';