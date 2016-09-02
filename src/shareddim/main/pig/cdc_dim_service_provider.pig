/*######################################################################################################### 
PIG SCRIPT				:cdc_dim_service_provider.pig
AUTHOR					:Pig script is auto generated with java utility.
DATE					:Wed Aug 24 14:16:07 UTC 2016
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
					MD5((chararray)CONCAT((chararray)(legacy_spid IS NULL ? 'null' : (chararray)legacy_spid),(chararray)(new_world_spid IS NULL ? 'null' : (chararray)new_world_spid))) as (md5_key_value:chararray),
					MD5((chararray)CONCAT((company_nm IS NULL ? 'null' : (chararray)company_nm),(service_provider_group_type IS NULL ? 'null' : (chararray)service_provider_group_type),(city IS NULL ? 'null' : (chararray)city),(state IS NULL ? 'null' : (chararray)state),(postal_code IS NULL ? 'null' : (chararray)postal_code),(is_excluded IS NULL ? 'null' : (chararray)is_excluded),(web_advertiser IS NULL ? 'null' : (chararray)web_advertiser),(call_center_advertiser IS NULL ? 'null' : (chararray)call_center_advertiser),(pub_advertiser IS NULL ? 'null' : (chararray)pub_advertiser),(is_insured IS NULL ? 'null' : (chararray)is_insured),(is_bonded IS NULL ? 'null' : (chararray)is_bonded),(is_licensed IS NULL ? 'null' : (chararray)is_licensed),(background_check IS NULL ? 'null' : (chararray)background_check),(ecommerce_status IS NULL ? 'null' : (chararray)ecommerce_status),(vintage IS NULL ? 'null' : (chararray)vintage),(market_key IS NULL ? 'null' : (chararray)market_key))) as (md5_non_key_value:chararray);

md5_target = FOREACH load_target
			GENERATE *,
					MD5((chararray)CONCAT((chararray)(legacy_spid IS NULL ? 'null' : (chararray)legacy_spid),(chararray)(new_world_spid IS NULL ? 'null' : (chararray)new_world_spid))) as (md5_key_value:chararray),
					MD5((chararray)CONCAT((company_nm IS NULL ? 'null' : (chararray)company_nm),(service_provider_group_type IS NULL ? 'null' : (chararray)service_provider_group_type),(city IS NULL ? 'null' : (chararray)city),(state IS NULL ? 'null' : (chararray)state),(postal_code IS NULL ? 'null' : (chararray)postal_code),(is_excluded IS NULL ? 'null' : (chararray)is_excluded),(web_advertiser IS NULL ? 'null' : (chararray)web_advertiser),(call_center_advertiser IS NULL ? 'null' : (chararray)call_center_advertiser),(pub_advertiser IS NULL ? 'null' : (chararray)pub_advertiser),(is_insured IS NULL ? 'null' : (chararray)is_insured),(is_bonded IS NULL ? 'null' : (chararray)is_bonded),(is_licensed IS NULL ? 'null' : (chararray)is_licensed),(background_check IS NULL ? 'null' : (chararray)background_check),(ecommerce_status IS NULL ? 'null' : (chararray)ecommerce_status),(vintage IS NULL ? 'null' : (chararray)vintage),(market_key IS NULL ? 'null' : (chararray)market_key))) as (md5_non_key_value:chararray);


/*========Join source and target based on key columns========*/
joined_source_target = JOIN md5_source BY (legacy_spid, new_world_spid) FULL, md5_target BY (legacy_spid, new_world_spid);


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
							insert_records_for_sk::rank_insert_records + filter_load_max_sk::max_sk AS service_provider_key,
							insert_records_for_sk::md5_source::legacy_spid AS legacy_spid,
							insert_records_for_sk::md5_source::new_world_spid AS new_world_spid,
							insert_records_for_sk::md5_source::company_nm AS company_nm,
							insert_records_for_sk::md5_source::service_provider_group_type AS service_provider_group_type,
							insert_records_for_sk::md5_source::entered_dt AS entered_dt,
							insert_records_for_sk::md5_source::city AS city,
							insert_records_for_sk::md5_source::state AS state,
							insert_records_for_sk::md5_source::postal_code AS postal_code,
							insert_records_for_sk::md5_source::is_excluded AS is_excluded,
							insert_records_for_sk::md5_source::web_advertiser AS web_advertiser,
							insert_records_for_sk::md5_source::call_center_advertiser AS call_center_advertiser,
							insert_records_for_sk::md5_source::pub_advertiser AS pub_advertiser,
							insert_records_for_sk::md5_source::is_insured AS is_insured,
							insert_records_for_sk::md5_source::is_bonded AS is_bonded,
							insert_records_for_sk::md5_source::is_licensed AS is_licensed,
							insert_records_for_sk::md5_source::background_check AS background_check,
							insert_records_for_sk::md5_source::ecommerce_status AS ecommerce_status,
							insert_records_for_sk::md5_source::vintage AS vintage,
							insert_records_for_sk::md5_source::market_key AS market_key,
							'I' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========Generating updated records. action_cd added. surrogate_key picked from target========*/
update_records_final = FOREACH update_records GENERATE
						md5_target::service_provider_key AS service_provider_key,
							md5_source::legacy_spid AS legacy_spid,
							md5_source::new_world_spid AS new_world_spid,
							md5_source::company_nm AS company_nm,
							md5_source::service_provider_group_type AS service_provider_group_type,
							md5_source::entered_dt AS entered_dt,
							md5_source::city AS city,
							md5_source::state AS state,
							md5_source::postal_code AS postal_code,
							md5_source::is_excluded AS is_excluded,
							md5_source::web_advertiser AS web_advertiser,
							md5_source::call_center_advertiser AS call_center_advertiser,
							md5_source::pub_advertiser AS pub_advertiser,
							md5_source::is_insured AS is_insured,
							md5_source::is_bonded AS is_bonded,
							md5_source::is_licensed AS is_licensed,
							md5_source::background_check AS background_check,
							md5_source::ecommerce_status AS ecommerce_status,
							md5_source::vintage AS vintage,
							md5_source::market_key AS market_key,
							'U' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========Generating no change and delete records. action_id added. And EDH_BUS_MONTH picked from target========*/
no_change_delete_records_final = FOREACH no_change_delete_records GENERATE
								md5_target::service_provider_key AS service_provider_key,
							md5_target::legacy_spid AS legacy_spid,
							md5_target::new_world_spid AS new_world_spid,
							md5_target::company_nm AS company_nm,
							md5_target::service_provider_group_type AS service_provider_group_type,
							md5_target::entered_dt AS entered_dt,
							md5_target::city AS city,
							md5_target::state AS state,
							md5_target::postal_code AS postal_code,
							md5_target::is_excluded AS is_excluded,
							md5_target::web_advertiser AS web_advertiser,
							md5_target::call_center_advertiser AS call_center_advertiser,
							md5_target::pub_advertiser AS pub_advertiser,
							md5_target::is_insured AS is_insured,
							md5_target::is_bonded AS is_bonded,
							md5_target::is_licensed AS is_licensed,
							md5_target::background_check AS background_check,
							md5_target::ecommerce_status AS ecommerce_status,
							md5_target::vintage AS vintage,
							md5_target::market_key AS market_key,
							'NCD' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========union of insert and update and unchanged and delete records========*/
final_records_to_store = UNION insert_records_final, update_records_final, no_change_delete_records_final;


/*========Store the records into work table========*/
STORE final_records_to_store INTO '$WORK_SHARED_DIM_DB.$WORK_DIM_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatStorer();
