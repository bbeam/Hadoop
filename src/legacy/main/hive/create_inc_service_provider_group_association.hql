--/*
--  HIVE SCRIPT  : create_inc_service_provider_group_association.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 01, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProviderGroupAssociation). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_inc_service_provider_group_association.hql \
-- -hivevar LEGACY_INCOMING_DB=$LEGACY_INCOMING_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_service_provider_group_association
(
  service_provider_group_association_id STRING,
  service_provider_group_id STRING,
  sp_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_service_provider_group_association';