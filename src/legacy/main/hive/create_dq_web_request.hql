--/*
--  HIVE SCRIPT  : create_dq_web_request.hql
--  AUTHOR       : Gaurav Maheshwari
--  DATE         : Aug 02, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_web_request). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/legacy/main/hive/create_dq_web_request.hql \
--					--hivevar LEGACY_GOLD_DB="${LEGACY_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_legacy="${SOURCE_legacy}"
--*/

--  Creating a DQ hive table(inc_web_request) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:legacy_GOLD_DB}.dq_web_request
( 
	request_id VARCHAR(64),
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
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_web_request';


	