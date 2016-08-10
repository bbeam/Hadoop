--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_user_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_user_address). 
--*/

--  Creating a incoming hive table(inc_t_user_address) over the incoming data

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_user_address
(
	user_address_id INT,
	postal_address_id INT,
	user_id INT,
	is_primary TINYINT,
	al_id INT,
	location_description STRING,
	version INT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_user_address';