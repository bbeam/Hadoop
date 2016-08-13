--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_member_primary_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.ServiceProvider). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_member_primary_address
(
	member_primary_address_id STRING ,
	member_id STRING ,
	member_address_id STRING ,
	known_invalid_postal_address STRING ,
	create_date STRING ,
	create_by STRING,
	update_date STRING ,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_member_primary_address';