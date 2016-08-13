--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_member_permission.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_member_permission) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_member_permission
(
  member_permission_id STRING,
  user_id STRING,
  name STRING,
  value STRING,
  start_date_time STRING,
  end_date_time STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_member_permission';
