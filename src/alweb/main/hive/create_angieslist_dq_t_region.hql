--/*
--  HIVE SCRIPT  : create_dq_t_region.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngiesList.t_City). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_region
(
  region_id INT,
  country_id INT,
  name VARCHAR(128),
  abbreviation CHAR(2),
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_region';