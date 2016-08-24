/*######################################################################################################### 
PIG SCRIPT				:cdc_dim_members.pig
AUTHOR					:Pig script is auto generated with java utility.
DATE					:Wed Aug 24 14:15:16 UTC 2016
DESCRIPTION				:Pig script for SCD.
#########################################################################################################*/


/*========Register datafu jar for md5 calulation========*/
register $S3_DATAFU_JAR_LOACTION;
define MD5 datafu.pig.hash.MD5();


/*========Pull base and scd tables from into pig using HCatalog========*/
load_source = LOAD '$WORK_SHARED_DIM_DB.$TF_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatLoader();
load_target = LOAD '$GOLD_SHARED_DIM_DB.$TRGT_DIM_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatLoader();


/*========Generate md5 values for key and non key values along with other columns========*/
md5_source = FOREACH load_source
			GENERATE *,
					MD5((chararray)CONCAT((chararray)(member_id IS NULL ? 'null' : (chararray)member_id),(chararray)(user_id IS NULL ? 'null' : (chararray)user_id))) as (md5_key_value:chararray),
					MD5((chararray)CONCAT((email IS NULL ? 'null' : (chararray)email),(postal_code IS NULL ? 'null' : (chararray)postal_code),(pay_status IS NULL ? 'null' : (chararray)pay_status),(member_status IS NULL ? 'null' : (chararray)member_status),(expiration_status IS NULL ? 'null' : (chararray)expiration_status),(membership_tier_nm IS NULL ? 'null' : (chararray)membership_tier_nm),(primary_phone_number IS NULL ? 'null' : (chararray)primary_phone_number),(first_nm IS NULL ? 'null' : (chararray)first_nm),(last_nm IS NULL ? 'null' : (chararray)last_nm),(associate IS NULL ? 'null' : (chararray)associate),(employee IS NULL ? 'null' : (chararray)employee),(market_key IS NULL ? 'null' : (chararray)market_key),(member_dt IS NULL ? 'null' : ToString(member_dt)))) as (md5_non_key_value:chararray);

md5_target = FOREACH load_target
			GENERATE *,
					MD5((chararray)CONCAT((chararray)(member_id IS NULL ? 'null' : (chararray)member_id),(chararray)(user_id IS NULL ? 'null' : (chararray)user_id))) as (md5_key_value:chararray),
					MD5((chararray)CONCAT((email IS NULL ? 'null' : (chararray)email),(postal_code IS NULL ? 'null' : (chararray)postal_code),(pay_status IS NULL ? 'null' : (chararray)pay_status),(member_status IS NULL ? 'null' : (chararray)member_status),(expiration_status IS NULL ? 'null' : (chararray)expiration_status),(membership_tier_nm IS NULL ? 'null' : (chararray)membership_tier_nm),(primary_phone_number IS NULL ? 'null' : (chararray)primary_phone_number),(first_nm IS NULL ? 'null' : (chararray)first_nm),(last_nm IS NULL ? 'null' : (chararray)last_nm),(associate IS NULL ? 'null' : (chararray)associate),(employee IS NULL ? 'null' : (chararray)employee),(market_key IS NULL ? 'null' : (chararray)market_key),(member_dt IS NULL ? 'null' : ToString(member_dt)))) as (md5_non_key_value:chararray);


/*========Join source and target based on key columns========*/
joined_source_target = JOIN md5_source BY (member_id, user_id) FULL, md5_target BY (member_id, user_id);


/*========Split the records into insert_records, update_records and no_change_delete_records using md5_key and md5_non_key of source and target========*/
SPLIT joined_source_target INTO
					 insert_records IF (md5_target::md5_key_value IS NULL),
					 update_records IF ((md5_source::md5_non_key_value!=md5_target::md5_non_key_value) AND (md5_target::md5_key_value IS NOT NULL)),
					 no_change_delete_records IF ((md5_source::md5_non_key_value==md5_target::md5_non_key_value) OR (md5_source::md5_key_value IS NULL));


/*========surrogate key generation logic for insert_records========*/
/*========Pull the previous max surrogate key from surrogate_key_map table========*/
load_max_sk = LOAD  '$OPERATIONS_COMMON_DB.surrogate_key_map' USING org.apache.hive.hcatalog.pig.HCatLoader();
filter_load_max_sk = FILTER load_max_sk BY table_name =='$TRGT_DIM_TABLE_NAME';
insert_records_for_sk = RANK insert_records;
join_insert_records_for_sk_max = CROSS insert_records_for_sk,filter_load_max_sk;


/*========Generating new insert records. action_cd field added========*/
insert_records_final = FOREACH join_insert_records_for_sk_max GENERATE
							insert_records_for_sk::rank_insert_records + filter_load_max_sk::max_sk AS member_key,
							insert_records_for_sk::md5_source::member_id AS member_id,
							insert_records_for_sk::md5_source::user_id AS user_id,
							insert_records_for_sk::md5_source::email AS email,
							insert_records_for_sk::md5_source::postal_code AS postal_code,
							insert_records_for_sk::md5_source::pay_status AS pay_status,
							insert_records_for_sk::md5_source::member_status AS member_status,
							insert_records_for_sk::md5_source::expiration_status AS expiration_status,
							insert_records_for_sk::md5_source::member_dt AS member_dt,
							insert_records_for_sk::md5_source::membership_tier_nm AS membership_tier_nm,
							insert_records_for_sk::md5_source::primary_phone_number AS primary_phone_number,
							insert_records_for_sk::md5_source::first_nm AS first_nm,
							insert_records_for_sk::md5_source::last_nm AS last_nm,
							insert_records_for_sk::md5_source::associate AS associate,
							insert_records_for_sk::md5_source::employee AS employee,
							insert_records_for_sk::md5_source::market_key AS market_key,
							'I' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========Generating updated records. action_cd added. surrogate_key picked from target========*/
update_records_final = FOREACH update_records GENERATE
						md5_target::member_key AS member_key,
							md5_source::member_id AS member_id,
							md5_source::user_id AS user_id,
							md5_source::email AS email,
							md5_source::postal_code AS postal_code,
							md5_source::pay_status AS pay_status,
							md5_source::member_status AS member_status,
							md5_source::expiration_status AS expiration_status,
							md5_source::member_dt AS member_dt,
							md5_source::membership_tier_nm AS membership_tier_nm,
							md5_source::primary_phone_number AS primary_phone_number,
							md5_source::first_nm AS first_nm,
							md5_source::last_nm AS last_nm,
							md5_source::associate AS associate,
							md5_source::employee AS employee,
							md5_source::market_key AS market_key,
							'U' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========Generating no change and delete records. action_id added. And EDH_BUS_MONTH picked from target========*/
no_change_delete_records_final = FOREACH no_change_delete_records GENERATE
								md5_target::member_key AS member_key,
							md5_target::member_id AS member_id,
							md5_target::user_id AS user_id,
							md5_target::email AS email,
							md5_target::postal_code AS postal_code,
							md5_target::pay_status AS pay_status,
							md5_target::member_status AS member_status,
							md5_target::expiration_status AS expiration_status,
							md5_target::member_dt AS member_dt,
							md5_target::membership_tier_nm AS membership_tier_nm,
							md5_target::primary_phone_number AS primary_phone_number,
							md5_target::first_nm AS first_nm,
							md5_target::last_nm AS last_nm,
							md5_target::associate AS associate,
							md5_target::employee AS employee,
							md5_target::market_key AS market_key,
							'NCD' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========union of insert and update and unchanged and delete records========*/
final_records_to_store = UNION insert_records_final, update_records_final, no_change_delete_records_final;


/*========Store the records into work table========*/
STORE final_records_to_store INTO '$WORK_SHARED_DIM_DB.$WORK_DIM_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatStorer();
