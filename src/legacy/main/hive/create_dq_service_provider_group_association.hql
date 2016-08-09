--/*
--  HIVE SCRIPT  : create_dq_service_provider_group_association.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive DQ table(angie.ServiceProviderGroupAssociation). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_service_provider_group_association
(
  service_provider_group_association_id INT,
  service_provider_group_id INT,
  sp_id INT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_service_provider_group_association';
