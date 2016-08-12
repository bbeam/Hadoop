--/*
--  HIVE SCRIPT  : create_angieslist_dq_t_user.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_t_user). 
--*/

--  Creating a DQ hive table(dq_t_user) over the DQ data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_user
(
	user_id INT,
	first_name VARCHAR(128),
	middle_name VARCHAR(128),
	last_name VARCHAR(191),
	al_id INT,
	email VARCHAR(128), 
	password_hash CHAR(60), 
	status STRING, 
	test_user TINYINT,
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_user';