--/*
--  HIVE SCRIPT  : create_dq_t_city.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngiesList.t_City). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_city
(
  city_id INT,
  region_id INT,
  name VARCHAR(254),
  abbreviation CHAR(3),
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_city';