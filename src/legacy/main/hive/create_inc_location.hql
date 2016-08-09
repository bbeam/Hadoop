--/*
--  HIVE SCRIPT  : create_inc_location.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Jun 27, 2016
--  DESCRIPTION  : Creation of hive incoming table(Angie.Location). 
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_location
(
  location_id STRING,
  location_name STRING,
  location_description STRING,
  eligible_lead_months STRING,
  allow_ad_discount_below_minimum STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_location';