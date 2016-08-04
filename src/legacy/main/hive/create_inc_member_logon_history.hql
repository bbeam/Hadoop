--/*
--  HIVE SCRIPT  : create_inc_member_logon_history.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_member_logon_history). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/legacy/main/hive/create_inc_member_logon_history.hql \
							--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
							--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}" \
							--hivevar S3_BUCKET="${S3_BUCKET}"
--*/


--  Creating a incoming hive table(inc_member_logon_history) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_member_logon_history
(
	member_logon_history_id STRING,
	member_id STRING,
	ip_address STRING,
	logon_date STRING,
	user_agent_string STRING

)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_member_logon_history';