--/*
--  HIVE SCRIPT  : create_dq_t_member_permission.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive dq table(AngiesList.t_MemberPermission) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_dq_t_member_permission.hql \
-- -hivevar ALWEB_GOLD_DB=$ALWEB_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_member_permission
(
  member_permission_id INT,
  user_id INT,
  name varchar(254),
  value TINYINT ,
  start_date_time TIMESTAMP,
  end_date_time TIMESTAMP,
  create_date TIMESTAMP,
  create_by INT,
  update_date TIMESTAMP,
  update_by INT
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_member_permission';
