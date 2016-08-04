--/*
--  HIVE SCRIPT  : create_dq_member_logon_history.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_member_logon_history). 
--  USAGE        : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_dq_member_logon_history.hql \
							--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_member_logon_history) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_member_logon_history
(
	member_logon_history_id INT,
	member_id INT,
	ip_address BIGINT,
	logon_date TIMESTAMP,
	user_agent_string STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_member_logon_history';