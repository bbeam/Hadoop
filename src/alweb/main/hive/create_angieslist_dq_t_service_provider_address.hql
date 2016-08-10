--/*
--  HIVE SCRIPT  : create_angieslist_dq_t_service_provider_address.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngiesList.t_ServiceProviderAddress). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_service_provider_address
(
  service_provider_address_id INT,
  al_id INT,
  postal_address_id INT,
  service_provider_id INT,
  is_primary TINYINT,
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
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_service_provider_address';