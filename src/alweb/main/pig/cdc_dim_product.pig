/*
PIG SCRIPT    : cdc_dim_product.pig
AUTHOR        : The pig script got generate from util_generate_pig_SCD2.sh 
DATE          : JUL 04, 2016
DESCRIPTION   : Performs CDC , appends surrogate key and performs SDC.
*/

/*-----Register datafu jar for md5 calulation------*/

register file:/var/tmp/datafu-1.2.0.jar;

define MD5 datafu.pig.hash.MD5();

/*-----Pull base and scd tables from into pig using HCatalog------*/

load_source = LOAD  'work.tf_dim_product' USING org.apache.hive.hcatalog.pig.HCatLoader();

load_master = LOAD  'alwebmetrics_Gold.dim_product' USING org.apache.hive.hcatalog.pig.HCatLoader();

/*-----Pull max product_key processed for the table to start from max + 1------*/

load_max_sk = LOAD  'alwebmetrics_Gold.sk_map' USING org.apache.hive.hcatalog.pig.HCatLoader();

filter_load_max_sk = FILTER load_max_sk BY table_name =='dim_product';

/*-----Generate md5 values for key and non key values along with other columns------*/

md5_source = FOREACH load_source GENERATE *,MD5((chararray)source_ak) as (md5_key_value:chararray),MD5((chararray)CONCAT((chararray)source_table,(chararray)source_column,(chararray)master_product_group,(chararray)product_type,(chararray)product,(chararray)unit_price,(chararray)source,(chararray)joins_flag,(chararray)renewals_flag)) as (md5_non_key_value:chararray);

/*-----Filter out Current Active Records to compare and check old or new records------*/

fltr_actv_master = FILTER load_master BY current_active_ind =='Y';

/*-----Join new base table with the current active records based on key columns to split data same,new(insert) or changed(update or delete records)------*/

join_source_master = JOIN md5_source BY (source_ak) FULL,fltr_actv_master BY (source_ak);

SPLIT join_source_master INTO
       same_record IF (md5_source::md5_non_key_value==fltr_actv_master::md5_non_key_value),
       update_delete_record IF (((NOT(md5_source::md5_non_key_value==fltr_actv_master::md5_non_key_value)) AND fltr_actv_master::md5_key_value IS NOT NULL) OR md5_source::md5_key_value IS NULL),
       insert_record IF (fltr_actv_master::md5_key_value is null);

/*-----Fetch all the partitions from the scd table associated with new and changed records------*/

--Start

union_insert_upt_del = UNION update_delete_record,insert_record ;

sel_partitoned_val = FOREACH union_insert_upt_del GENERATE loadmonth as loadmonth;

dist_partition= DISTINCT sel_partitoned_val;

join_dist_partition= JOIN dist_partition by loadmonth,load_master by loadmonth;

--End

/*-----In Section will create below kind of records for pulled partition                                                                            -------------*/

/*-----1.Keep all the unaltered records as it is                                                                                                    -------------*/

/*-----2.For Deleted records closed the eff_end_dt and as current_date the status of current active records and kepp the history records as it is   -------------*/

/*-----3.For updates only process the history records as the current active records will closed using timestamp column present in the update        -------------*/

/*-----  records present in the base table.This is covered in the next section                                                                       -------------*/

/***************************************Section 1 starts ****************************************************************/

join_cust_id = JOIN join_dist_partition by (load_master::source_ak) LEFT , update_delete_record by (fltr_actv_master::source_ak);

SPLIT join_cust_id INTO
 unaltered_record IF (update_delete_record::fltr_actv_master::md5_key_value is null AND md5_source::md5_key_value IS NULL),
 altered_delete_record IF (NOT(update_delete_record::fltr_actv_master::md5_key_value is null) AND md5_source::md5_key_value IS NULL),
 altered_update_record IF (NOT(update_delete_record::fltr_actv_master::md5_key_value is null) AND join_dist_partition::load_master::current_active_ind != 'Y' AND md5_source::md5_key_value IS NOT NULL);

/***************************************Section 1.a ****************************************************************/

partiton_unaltered_records= FOREACH unaltered_record GENERATE
join_dist_partition::load_master::source_ak AS source_ak,
join_dist_partition::load_master::source_table AS source_table,
join_dist_partition::load_master::source_column AS source_column,
join_dist_partition::load_master::product_key AS product_key,
join_dist_partition::load_master::master_product_group AS master_product_group,
join_dist_partition::load_master::product_type AS product_type,
join_dist_partition::load_master::product AS product,
join_dist_partition::load_master::unit_price AS unit_price,
join_dist_partition::load_master::source AS source,
join_dist_partition::load_master::joins_flag AS joins_flag,
join_dist_partition::load_master::renewals_flag AS renewals_flag,
ToString(join_dist_partition::load_master::eff_start_dt, 'yyyy-MM-dd') AS eff_start_dt,
ToString(join_dist_partition::load_master::eff_end_dt, 'yyyy-MM-dd') AS eff_end_dt,
join_dist_partition::load_master::current_active_ind AS current_active_ind,
join_dist_partition::load_master::md5_non_key_value AS md5_non_key_value,
join_dist_partition::load_master::md5_key_value AS md5_key_value,
join_dist_partition::load_master::loadmonth AS loadmonth;

/***************************************Section 1.b ****************************************************************/

partiton_altered_update_records= FOREACH altered_update_record GENERATE
join_dist_partition::load_master::source_ak AS source_ak,
join_dist_partition::load_master::source_table AS source_table,
join_dist_partition::load_master::source_column AS source_column,
join_dist_partition::load_master::product_key AS product_key,
join_dist_partition::load_master::master_product_group AS master_product_group,
join_dist_partition::load_master::product_type AS product_type,
join_dist_partition::load_master::product AS product,
join_dist_partition::load_master::unit_price AS unit_price,
join_dist_partition::load_master::source AS source,
join_dist_partition::load_master::joins_flag AS joins_flag,
join_dist_partition::load_master::renewals_flag AS renewals_flag,
ToString(join_dist_partition::load_master::eff_start_dt, 'yyyy-MM-dd') AS eff_start_dt,
ToString(join_dist_partition::load_master::eff_end_dt, 'yyyy-MM-dd') AS eff_end_dt,
join_dist_partition::load_master::current_active_ind AS current_active_ind,
join_dist_partition::load_master::md5_non_key_value AS md5_non_key_value,
join_dist_partition::load_master::md5_key_value AS md5_key_value,
join_dist_partition::load_master::loadmonth AS loadmonth;

/***************************************Section 1.c ****************************************************************/

partiton_altered_delete_records= FOREACH altered_delete_record GENERATE
join_dist_partition::load_master::source_ak AS source_ak,
join_dist_partition::load_master::source_table AS source_table,
join_dist_partition::load_master::source_column AS source_column,
join_dist_partition::load_master::product_key AS product_key,
join_dist_partition::load_master::master_product_group AS master_product_group,
join_dist_partition::load_master::product_type AS product_type,
join_dist_partition::load_master::product AS product,
join_dist_partition::load_master::unit_price AS unit_price,
join_dist_partition::load_master::source AS source,
join_dist_partition::load_master::joins_flag AS joins_flag,
join_dist_partition::load_master::renewals_flag AS renewals_flag,
ToString(join_dist_partition::load_master::eff_start_dt, 'yyyy-MM-dd') AS eff_start_dt,
(join_dist_partition::load_master::current_active_ind=='Y'?ToString(join_dist_partition::load_master::eff_start_dt, 'yyyy-MM-dd'):ToString(CurrentTime(), 'yyyy-MM-dd')) AS eff_end_dt,
(join_dist_partition::load_master::current_active_ind=='Y'?'N':join_dist_partition::load_master::current_active_ind) AS current_active_ind,
join_dist_partition::load_master::md5_non_key_value AS md5_non_key_value,
join_dist_partition::load_master::md5_key_value AS md5_key_value,
join_dist_partition::load_master::loadmonth AS loadmonth;

/***************************************Section 1.d ****************************************************************/

union_partition= UNION partiton_unaltered_records,partiton_altered_update_records,partiton_altered_delete_records;

/***************************************Section 1 ends ****************************************************************/

/*-----This Section will generate below kind of records for pulled partition                                                                                  -------------*/

/*-----1.Close the efff_end_dt and update current_active_ind status of the current active records                                                             -------------*/

/*-----2.Generate a new record as current active from the record received from base table with  current_active_ind as 'Y' and efff_end_dt as 9999-12-31       -------------*/

/***************************************Section 2 starts ****************************************************************/

filter_update_record = FILTER update_delete_record  BY  NOT(md5_source::md5_key_value IS NULL);

/***************************************Section 2.a ****************************************************************/

update_record_source = FOREACH filter_update_record GENERATE
md5_source::source_ak AS source_ak,
md5_source::source_table AS source_table,
md5_source::source_column AS source_column,
fltr_actv_master::product_key AS product_key,
md5_source::master_product_group AS master_product_group,
md5_source::product_type AS product_type,
md5_source::product AS product,
md5_source::unit_price AS unit_price,
md5_source::source AS source,
md5_source::joins_flag AS joins_flag,
md5_source::renewals_flag AS renewals_flag,
ToString(CurrentTime(), 'yyyy-MM-dd') AS eff_start_dt,
ToString(ToDate('9999-12-31'),'yyyy-MM-dd') as (eff_end_dt:chararray),
'Y' as current_active_ind,
md5_source::md5_non_key_value as md5_non_key_value,
md5_source::md5_key_value as md5_key_value,
ToString(CurrentTime(),'yyyyMM') AS loadmonth;

/***************************************Section 2.b ****************************************************************/

update_record_master = FOREACH filter_update_record GENERATE 
fltr_actv_master::source_ak AS source_ak,
fltr_actv_master::source_table AS source_table,
fltr_actv_master::source_column AS source_column,
fltr_actv_master::product_key AS product_key,
fltr_actv_master::master_product_group AS master_product_group,
fltr_actv_master::product_type AS product_type,
fltr_actv_master::product AS product,
fltr_actv_master::unit_price AS unit_price,
fltr_actv_master::source AS source,
fltr_actv_master::joins_flag AS joins_flag,
fltr_actv_master::renewals_flag AS renewals_flag,
ToString(fltr_actv_master::eff_start_dt, 'yyyy-MM-dd') AS eff_start_dt,
ToString(CurrentTime(), 'yyyy-MM-dd') AS eff_end_dt,
'N' AS current_active_ind,
fltr_actv_master::md5_non_key_value AS md5_non_key_value,
fltr_actv_master::md5_key_value AS md5_key_value,
fltr_actv_master::loadmonth AS partition_col;

/***************************************Section 2 ends ****************************************************************/

/*-----This Section will generate below kind of records                                                                                   -------------*/

/*-----1.Generate the surregate key for the newly inserted records                                                                        -------------*/

/*-----2.Generate a record  for newly inserts as current active from the record received from base table withcurrent_active_ind as 'Y' and efff_end_dt as 9999-12-31-------------*/

/***************************************Section 3 starts ****************************************************************/

insert_record_sk = RANK insert_record;

join_insert_record_sk_max = CROSS insert_record_sk,filter_load_max_sk;

insert_record_source= FOREACH join_insert_record_sk_max GENERATE 
insert_record_sk::md5_source::source_ak AS source_ak,
insert_record_sk::md5_source::source_table AS source_table,
insert_record_sk::md5_source::source_column AS source_column,
insert_record_sk::rank_insert_record + filter_load_max_sk::max_sk AS product_key,
insert_record_sk::md5_source::master_product_group AS master_product_group,
insert_record_sk::md5_source::product_type AS product_type,
insert_record_sk::md5_source::product AS product,
insert_record_sk::md5_source::unit_price AS unit_price,
insert_record_sk::md5_source::source AS source,
insert_record_sk::md5_source::joins_flag AS joins_flag,
insert_record_sk::md5_source::renewals_flag AS renewals_flag,
ToString(CurrentTime(), 'yyyy-MM-dd') AS eff_start_dt,
ToString(ToDate('9999-12-31'),'yyyy-MM-dd') as (eff_end_dt:chararray),
'Y' as current_active_ind,
insert_record_sk::md5_source::md5_non_key_value AS md5_non_key_value,
insert_record_sk::md5_source::md5_key_value AS md5_key_value,
ToString(CurrentTime(),'yyyyMM') AS loadmonth;

/***************************************Section 3 ends ****************************************************************/

/*-----This Section will union all the generated records and make final set of records     -------------*/

/***************************************Section 4 starts ****************************************************************/

union_all= UNION insert_record_source,update_record_source,update_record_master,union_partition;

final_records = FOREACH union_all GENERATE 
source_ak AS source_ak,
source_table AS source_table,
source_column AS source_column,
product_key As product_key,
master_product_group AS master_product_group,
product_type AS product_type,
product AS product,
unit_price AS unit_price,
source AS source,
joins_flag AS joins_flag,
renewals_flag AS renewals_flag,
ToDate(eff_start_dt) AS eff_start_dt,
ToDate(eff_end_dt) AS eff_end_dt,
current_active_ind AS current_active_ind,
md5_non_key_value AS md5_non_key_value,
md5_key_value AS md5_key_value,
loadmonth AS loadmonth;

/***************************************Section 4 ends ****************************************************************/

/*-----Load the final recors into the intermediate table     -------------*/

/***************************************Section 6 starts ****************************************************************/

STORE final_records INTO 'work.dim_product_tmp' USING org.apache.hive.hcatalog.pig.HCatStorer();

/***************************************Section 6 ends ****************************************************************/