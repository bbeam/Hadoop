--/*
--  HIVE SCRIPT  : create_inc_t_region.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_region). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_region
(
  region_id STRING,
  country_id STRING,
  name STRING,
  abbreviation STRING,
  version STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_region';