--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_service_provider_group_association.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 01, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProviderGroupAssociation). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_service_provider_group_association
(
  service_provider_group_association_id STRING,
  service_provider_group_id STRING,
  sp_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_service_provider_group_association';