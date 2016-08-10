--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_web_request.hql
--  AUTHOR       : Gaurav maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_web_request). 
--*/

--  Creating a incoming hive table(inc_web_request) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_web_request
(
	request_id STRING,
	time_utc STRING,    
	verb STRING,   
	path STRING,
	query STRING,
	host STRING,
	port STRING,
	user_agent STRING,     
	user_ip STRING,
	x_forwarded_for STRING,
	response_status STRING,
    response_sub_status STRING,
	source_param STRING,
	member_id STRING
	)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/weblogging/dbo/incremental/daily/inc_web_request';