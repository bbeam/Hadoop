--/*
--  HIVE SCRIPT  : create_inc_t_city.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_city). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_inc_t_city.hql \
-- -hivevar ALWEB_INCOMING_DB=$ALWEB_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_t_city
(
  city_id STRING,
  region_id STRING,
  name STRING,
  abbreviation STRING,
  version STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_city';