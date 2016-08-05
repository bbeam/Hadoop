--/*
--  HIVE SCRIPT  : create_dq_service_provider_group_association.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.ServiceProviderGroupAssociation). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_service_provider_group_association.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \
-- -hivevar S3_BUCKET=$S3_BUCKET \
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_service_provider_group_association
(
  service_provider_group_association_id INT,
  service_provider_group_id INT,
  sp_id INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_service_provider_group_association';