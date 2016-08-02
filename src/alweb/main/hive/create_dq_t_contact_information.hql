--/*
--  HIVE SCRIPT  : create_dq_t_contact_information.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Aug 1, 2016
--  DESCRIPTION  : Creation of hive dq table(AngiesList.t_ContactInformation) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_dq_t_contact_information.hql \
-- -hivevar ALWEB_GOLD_DB=$ALWEB_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_contact_information
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
	create_date TIMESTAMP,
	create_by INT ,
	update_date TIMESTAMP,
	update_by INT, 
	load_timestamp	TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_contact_information';
