--/*
--  HIVE SCRIPT  : create_dq_members.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.Members) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_members
(
	member_id INT,
	mid INT,
	company STRING,
	comments STRING,
	email STRING,
	known_invalid_email TINYINT,
	employee STRING,
	est_expiration TIMESTAMP,
	expiration TIMESTAMP,
	extension STRING,
	fax STRING,
	first_name STRING,
	last_name STRING,
	gender STRING,
	home_phone STRING,
	market STRING,
	est_member_date TIMESTAMP,
	member_date TIMESTAMP,
	sign_up_id INT,
	source_id INT,
	status STRING,
	work_phone STRING,
	compliments STRING,
	b_send_auto_emails SMALLINT,
	b_text_only_emails SMALLINT,
	b_active_sent SMALLINT,
	external_id INT,
	est_create_date TIMESTAMP,
	create_date TIMESTAMP,
	auto_renewal_notification_indicator TINYINT,
	message_center_enabled TINYINT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_members';
