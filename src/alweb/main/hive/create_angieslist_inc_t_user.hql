--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_user.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_user). 
--*/

--  Creating a incoming hive table(inc_t_user) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_user
(
	user_id STRING,
	first_name STRING,
	middle_name STRING,
	last_name STRING,
	al_id STRING,
	email STRING, 
	password_hash STRING, 
	status STRING, 
	test_user STRING,
	version STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_user';