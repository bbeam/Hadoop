--/*
--  HIVE SCRIPT  : create_dq_member_logon_history.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_member_logon_history). 
--*/

--  Creating a dq hive table(dq_member_logon_history) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_member_logon_history
(
	member_logon_history_id INT,
	member_id INT,
	ip_address BIGINT,
	est_logon_date TIMESTAMP,
	logon_date TIMESTAMP,
	user_agent_string STRING,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_member_logon_history';
