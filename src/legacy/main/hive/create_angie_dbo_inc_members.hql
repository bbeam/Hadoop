--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_members.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 03, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.Members) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_members
(
	member_id STRING,
	mid STRING,
	company STRING,
	comments STRING,
	email STRING,
	known_invalid_email STRING,
	employee STRING,
	expiration STRING,
	extension STRING,
	fax STRING,
	first_name STRING,
	last_name STRING,
	gender STRING,
	home_phone STRING,
	market STRING,
	member_date STRING,
	sign_up_id STRING,
	source_id STRING,
	status STRING,
	work_phone STRING,
	compliments STRING,
	b_send_auto_emails STRING,
	b_text_only_emails STRING,
	b_active_sent STRING,
	external_id STRING,
	create_date STRING,
	auto_renewal_notification_indicator STRING,
	message_center_enabled STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_members';
