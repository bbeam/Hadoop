--/*
--  HIVE SCRIPT  : create_dq_t_member_permission.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive dq table(AngiesList.t_MemberPermission) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_member_permission
(
  member_permission_id INT,
  user_id INT,
  name varchar(254),
  value TINYINT ,
  est_start_date_time TIMESTAMP,
  start_date_time TIMESTAMP,
  est_end_date_time TIMESTAMP,
  end_date_time TIMESTAMP,
  est_create_date TIMESTAMP,
  create_date TIMESTAMP,
  create_by INT,
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  update_by INT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_member_permission';
