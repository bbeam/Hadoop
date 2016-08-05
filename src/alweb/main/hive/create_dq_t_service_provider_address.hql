--/*
--  HIVE SCRIPT  : create_dq_t_service_provider_address.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngiesList.t_ServiceProviderAddress). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_dq_t_service_provider_address.hql \
-- -hivevar ALWEB_GOLD_DB=$ALWEB_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_service_provider_address
(
  service_provider_address_id INT,
  al_id INT,
  postal_address_id INT,
  service_provider_id INT,
  is_primary TINYINT,
  version INT,
  create_date TIMESTAMP,
  create_by INT,
  update_date TIMESTAMP,
  update_by INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_service_provider_address';