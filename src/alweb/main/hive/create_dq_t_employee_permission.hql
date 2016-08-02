--/*
--  HIVE SCRIPT  : create_inc_t_employee_permission.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_employee_permission). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_t_employee_permission.hql \
--					--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a incoming hive table(inc_t_employee_permission) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.inc_t_employee_permission
(
	employee_permission_id INT,
	user_id INT,
	name VARCHAR(254),
	value TINYINT,
	start_datetime TIMESTAMP,
	end_datetime TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT,
	update_date TIMESTAMP,
	update_by INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/t_employee_permission';