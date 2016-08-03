--/*
--  HIVE SCRIPT  : create_inc_t_service_provider_address.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_ServiceProviderAddress). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_inc_service_provider.hql \
-- -hivevar ALWEB_INCOMING_DB=$ALWEB_INCOMING_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.t_service_provider_address
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/inc_t_service_provider_address';