/*######################################################################################################### 
PIG SCRIPT				:cdc_dim_market.pig
AUTHOR					:Pig script is auto generated with java utility.
DATE					:Thu Aug 11 14:32:28 UTC 2016
DESCRIPTION				:Pig script for SCD and loads data in work table.
							Then hive script will load data from work table to target dim_market table.
							This pig script and hive script are part of one wrapper script named: wrapper_cdc_target_dim_load.sh
#########################################################################################################*/



/*========Register datafu jar for md5 calulation========*/
register s3://al-edh-dev/src/common/main/lib/datafu-1.2.0.jar;
define MD5 datafu.pig.hash.MD5();


/*========Pull base and scd tables from into pig using HCatalog========*/
load_source = LOAD '$WORK_SHARED_DIM_DB.$TF_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatLoader();
load_target = LOAD '$GOLD_SHARED_DIM_DB.$TRGT_DIM_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatLoader();


/*========Generate md5 values for key and non key values along with other columns========*/
md5_source = FOREACH load_source
			GENERATE *,
					MD5((chararray)market_id) as (md5_key_value:chararray),
					MD5((chararray)market_nm) as (md5_non_key_value:chararray);

md5_target = FOREACH load_target
			GENERATE *,
					MD5((chararray)market_id) as (md5_key_value:chararray),
					MD5((chararray)market_nm) as (md5_non_key_value:chararray);


/*========Join source and target based on key columns========*/
joined_source_target = JOIN md5_source BY (market_id) FULL, md5_target BY (market_id);


/*========Split the records into insert_records, update_records and no_change_delete_records using md5_key and md5_non_key of source and target========*/
SPLIT joined_source_target INTO
					 insert_records IF (md5_target::md5_key_value IS NULL),
					 update_records IF ((md5_source::md5_non_key_value!=md5_target::md5_non_key_value) AND (md5_target::md5_key_value IS NOT NULL)),
					 no_change_delete_records IF ((md5_source::md5_non_key_value==md5_target::md5_non_key_value) OR (md5_source::md5_key_value IS NULL));


/*========surrogate key generation logic for insert_records========*/
/*========Pull the previous max surrogate key from ops_common.surrogate_key_map table========*/
load_max_sk = LOAD  '$OPERATIONS_COMMON_DB.surrogate_key_map' USING org.apache.hive.hcatalog.pig.HCatLoader();
filter_load_max_sk = FILTER load_max_sk BY table_name =='$TRGT_DIM_TABLE_NAME';
insert_records_for_sk = RANK insert_records;
join_insert_records_for_sk_max = CROSS insert_records_for_sk,filter_load_max_sk;


/*========Generating new insert records. action_cd and edh_bus_month fields added========*/
insert_records_final = FOREACH join_insert_records_for_sk_max GENERATE
							insert_records_for_sk::rank_insert_records + filter_load_max_sk::max_sk AS market_key,
							insert_records_for_sk::md5_source::market_id AS market_id,
							insert_records_for_sk::md5_source::market_nm AS market_nm,
							'I' AS action_cd,
							'$EDH_BUS_MONTH' AS edh_bus_month;

/*========Generating updated records. action_cd added. surrogate_key and edh_bus_month picked from target========*/
update_records_final = FOREACH update_records GENERATE
							md5_target::market_key AS market_key,
							md5_source::market_id AS market_id,
							md5_source::market_nm AS market_nm,
							'U' AS action_cd,
							md5_target::edh_bus_month AS edh_bus_month;


/*========union of insert and update records========*/
union_insert_update_records_final = UNION insert_records_final, update_records_final;


/*========finding distinct partition values for insert and update records only========*/
/*========This will be joined with no_change_delete_records to get no_change_delete_records from concerne partition.========*/
sel_partitoned_col_vals = FOREACH union_insert_update_records_final GENERATE edh_bus_month as edh_bus_month;
dist_partition_vals = DISTINCT sel_partitoned_col_vals;
join_dist_partition = JOIN dist_partition_vals by edh_bus_month, no_change_delete_records by edh_bus_month;


/*========Generating no change and delete records. action_id added. And edh_bus_month picked from target========*/
no_change_delete_records_final = FOREACH join_dist_partition GENERATE
							no_change_delete_records::md5_target::market_key AS market_key,
							no_change_delete_records::md5_target::market_id AS market_id,
							no_change_delete_records::md5_target::market_nm AS market_nm,
							'NCD' AS action_cd,
							no_change_delete_records::md5_target::edh_bus_month AS edh_bus_month;


/*========union of union_insert_update_records_final and no_change_delete_records_final========*/
final_records_to_store = UNION union_insert_update_records_final, no_change_delete_records_final;


/*========Store the records into work table========*/
STORE final_records_to_store INTO '$WORK_SHARED_DIM_DB.$WORK_DIM_TABLE_NAME' USING org.apache.hive.hcatalog.pig.HCatStorer();




