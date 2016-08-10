--/*
--  HIVE SCRIPT  : create_dq_web_request.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_web_request).
--*/

--  Creating a DQ hive table(inc_web_request) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_web_request
( 
	request_id VARCHAR(64),
	est_time_utc TIMESTAMP,
	time_utc TIMESTAMP,    
	verb STRING,   
	path STRING,
	query STRING,
	host STRING,
	port INT,
	user_agent STRING,     
	user_ip STRING,
	x_forwarded_for STRING,
	response_status INT,
    response_sub_status INT,
	source_param STRING,
	member_id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/weblogging/dbo/incremental/daily/dq_web_request';


	
