--/*
--  HIVE SCRIPT  : create_angieslist_dq_t_contact_information.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive dq table(AngiesList.t_ContactInformation) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_contact_information
(
	contact_information_id INT ,
	context_entity_id INT ,
	entity_type VARCHAR(254) ,
	is_primary TINYINT,
	primary_phonenumber VARCHAR(254),
	secondary_phonenumber VARCHAR(254),
	fax_number VARCHAR(254),
	first_name VARCHAR(254),
	middle_name VARCHAR(254),
	last_name VARCHAR(254),
	email VARCHAR(254),
	website varchar(2048),
	facebook VARCHAR(254),
	twitter VARCHAR(254),
	mobile_phone_number VARCHAR(254),
	version INT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	create_by INT ,
	est_update_date TIMESTAMP,	
	update_date TIMESTAMP,
	update_by INT, 
	est_load_timestamp	TIMESTAMP,
	utc_load_timestamp	TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/angieslist/full/daily/dq_t_contact_information';
