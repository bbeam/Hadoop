--/*
--  HIVE SCRIPT  : create_inc_t_associate_permission.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_associate_permission). 
--*/

--  Creating a incoming hive table(inc_t_associate_permission) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_associate_permission
(
	associate_permission_id INT,
	al_id INT,
	user_id INT,
	service_provider_id INT,
	name VARCHAR(254),
	value TINYINT,
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_associate_permission';