--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_service_provider_address.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_ServiceProviderAddress). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_service_provider_address
(
  service_provider_address_id STRING,
  al_id STRING,
  postal_address_id STRING,
  service_provider_id STRING,
  is_primary STRING,
  version STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_service_provider_address';