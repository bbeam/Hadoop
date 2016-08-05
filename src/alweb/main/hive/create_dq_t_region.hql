--/*
--  HIVE SCRIPT  : create_dq_t_region.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngiesList.t_City). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_dq_t_region.hql \
-- -hivevar ALWEB_GOLD_DB=$ALWEB_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_region
(
  region_id INT,
  country_id INT,
  name VARCHAR(128),
  abbreviation CHAR(2),
  version INT,
  create_date TIMESTAMP,
  create_by INT,
  update_date TIMESTAMP,
  update_by INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_region';