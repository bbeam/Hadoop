--/*
--  HIVE SCRIPT  : create_inc_t_member_permission.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_member_permission) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_inc_t_member_permission.hql \
-- -hivevar ALWEB_INCOMING_DB=$ALWEB_INCOMING_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_member_permission
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_member_permission';
