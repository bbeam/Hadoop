--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_location.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive DQ table(Angie.Location). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_location
(
  location_id INT,
  location_name STRING,
  location_description STRING,
  eligible_lead_months INT,
  allow_ad_discount_below_minimum TINYINT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_location';
