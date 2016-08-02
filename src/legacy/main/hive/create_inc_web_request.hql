--/*
--  HIVE SCRIPT  : create_inc_web_request.hql
--  AUTHOR       : Gaurav maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_web_request). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/$SOURCE_LEGACY/main/hive/create_inc_web_request.hql \
--					--hivevar LEGACY_INCOMING_DB=${LEGACY_INCOMING_DB} \
--					--hivevar S3_BUCKET=${S3_BUCKET} \
--					--hivevar SOURCE_LEGACY=${SOURCE_LEGACY}
--*/

--  Creating a incoming hive table(inc_web_request) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_web_request
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
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_web_request';