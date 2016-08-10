--/*
--  HIVE SCRIPT  : create_inc_t_contact_information.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_contact_information) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_contact_information
(

	contact_information_id STRING ,
	context_entity_id STRING ,
	entity_type STRING ,
	is_primary STRING,
	primary_phonenumber STRING,
	secondary_phonenumber STRING,
	fax_number STRING,
	first_name STRING,
	middle_name STRING,
	last_name STRING,
	email STRING,
	website STRING,
	facebook STRING,
	twitter STRING,
	mobile_phone_number STRING,
	version STRING,
	create_date STRING,
	create_by STRING ,
	update_date STRING,
	update_by STRING 
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_contact_information';
