--/*
--  HIVE SCRIPT  : create_inc_t_employee_permission.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_employee_permission). 
--*/

--  Creating a incoming hive table(inc_t_employee_permission) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_employee_permission
(
	employee_permission_id INT,
	user_id INT,
	name VARCHAR(254),
	value TINYINT,
	est_start_datetime TIMESTAMP,
	start_datetime TIMESTAMP,
	est_end_datetime TIMESTAMP,
	end_datetime TIMESTAMP,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT,
	est_update_date TIMESTAMP,
	update_date TIMESTAMP,
	update_by INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_employee_permission';