--/*
--  HIVE SCRIPT  : create_inc_t_user_address.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Jul 28, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_t_user_address). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/alweb/main/hive/create_dq_t_user_address.hql \
--					--hivevar ALWEB_GOLD_DB="${ALWEB_GOLD_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}"
--*/

--  Creating a incoming hive table(inc_t_user_address) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_user_address
(
	user_address_id INT,
	postal_address_id INT,
	user_id INT,
	is_primary TINYINT,
	al_id INT,
	location_description STRING,
	version INT,
	create_date TIMESTAMP,
	create_by INT,
	update_date TIMESTAMP,
	update_by INT,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angieslist/full/daily/dq_t_user_address';