/*######################################################################################################### 
PIG SCRIPT				:cdc_dim_product.pig
AUTHOR					:Pig script is auto generated with java utility.
DATE					:Sun Sep 04 08:30:13 UTC 2016
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
					MD5((chararray)CONCAT((chararray)(source_ak IS NULL ? 'null' : (chararray)source_ak),(chararray)(source_table IS NULL ? 'null' : (chararray)source_table))) as (md5_key_value:chararray),
					MD5((chararray)CONCAT((source_column IS NULL ? 'null' : (chararray)source_column),(master_product_group IS NULL ? 'null' : (chararray)master_product_group),(product_type IS NULL ? 'null' : (chararray)product_type),(product IS NULL ? 'null' : (chararray)product),(unit_price IS NULL ? 'null' : (chararray)unit_price),(source IS NULL ? 'null' : (chararray)source))) as (md5_non_key_value:chararray);

md5_target = FOREACH load_target
			GENERATE *,
					MD5((chararray)CONCAT((chararray)(source_ak IS NULL ? 'null' : (chararray)source_ak),(chararray)(source_table IS NULL ? 'null' : (chararray)source_table))) as (md5_key_value:chararray),
					MD5((chararray)CONCAT((source_column IS NULL ? 'null' : (chararray)source_column),(master_product_group IS NULL ? 'null' : (chararray)master_product_group),(product_type IS NULL ? 'null' : (chararray)product_type),(product IS NULL ? 'null' : (chararray)product),(unit_price IS NULL ? 'null' : (chararray)unit_price),(source IS NULL ? 'null' : (chararray)source))) as (md5_non_key_value:chararray);


/*========Join source and target based on key columns========*/
joined_source_target = JOIN md5_source BY md5_key_value FULL, md5_target BY md5_key_value;


/*========Split the records into insert_records, update_records and no_change_delete_records using md5_key and md5_non_key of source and target========*/
SPLIT joined_source_target INTO
					 insert_records IF (md5_target::md5_key_value IS NULL),
					 update_records IF ((md5_source::md5_non_key_value!=md5_target::md5_non_key_value) AND (md5_source::md5_key_value==md5_target::md5_key_value)),
					 no_change_delete_records IF ((md5_source::md5_non_key_value==md5_target::md5_non_key_value) OR (md5_source::md5_key_value IS NULL));

/*========surrogate key generation logic for insert_records========*/
/*========Pull the previous max surrogate key from surrogate_key_map table========*/
load_max_sk = LOAD  '$OPERATIONS_COMMON_DB.surrogate_key_map' USING org.apache.hive.hcatalog.pig.HCatLoader();
filter_load_max_sk = FILTER load_max_sk BY table_name =='$TRGT_DIM_TABLE_NAME';
insert_records_for_sk = RANK insert_records;
join_insert_records_for_sk_max = CROSS insert_records_for_sk,filter_load_max_sk;


/*========Generating new insert records. action_cd field added========*/
insert_records_final = FOREACH join_insert_records_for_sk_max GENERATE
							insert_records_for_sk::rank_insert_records + filter_load_max_sk::max_sk AS product_key,
							insert_records_for_sk::md5_source::source_ak AS source_ak,
							insert_records_for_sk::md5_source::source_table AS source_table,
							insert_records_for_sk::md5_source::source_column AS source_column,
							insert_records_for_sk::md5_source::master_product_group AS master_product_group,
							insert_records_for_sk::md5_source::product_type AS product_type,
							insert_records_for_sk::md5_source::product AS product,
							insert_records_for_sk::md5_source::unit_price AS unit_price,
							insert_records_for_sk::md5_source::source AS source,
							'I' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========Generating updated records. action_cd added. surrogate_key picked from target========*/
update_records_final = FOREACH update_records GENERATE
						md5_target::product_key AS product_key,
							md5_source::source_ak AS source_ak,
							md5_source::source_table AS source_table,
							md5_source::source_column AS source_column,
							md5_source::master_product_group AS master_product_group,
							md5_source::product_type AS product_type,
							md5_source::product AS product,
							md5_source::unit_price AS unit_price,
							md5_source::source AS source,
							'U' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========Generating no change and delete records. action_id added. And EDH_BUS_MONTH picked from target========*/
no_change_delete_records_final = FOREACH no_change_delete_records GENERATE
								md5_target::product_key AS product_key,
							md5_target::source_ak AS source_ak,
							md5_target::source_table AS source_table,
							md5_target::source_column AS source_column,
							md5_target::master_product_group AS master_product_group,
							md5_target::product_type AS product_type,
							md5_target::product AS product,
							md5_target::unit_price AS unit_price,
							md5_target::source AS source,
							'NCD' AS action_cd,
							ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
							ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/*========union of insert and update and unchanged and delete records========*/
final_records_to_store = UNION insert_records_final, update_records_final, no_change_delete_records_final;


/*========Store the records into work table========*/
STORE final_records_to_store INTO '$WORK_SHARED_DIM_DB.$WORK_DIM_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatStorer();
