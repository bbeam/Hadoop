--/*
--  HIVE SCRIPT  : create_inc_angieslistweb_prod_searched_sp.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_angieslistweb_prod_searched_sp). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_inc_angieslistweb_prod_searched_sp.hql \
--     --hivevar SEGMENT_INCOMING_DB="${SEGMENT_INCOMING_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 
--*/

--  Creating a incoming hive table(inc_angieslistweb_prod_searched_sp) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_INCOMING_DB}.inc_angieslistweb_prod_searched_sp
(
id STRING,
received_at STRING,
uuid STRING,
advertisers_on_page STRING,
context_auth_token STRING,
context_ip STRING,
context_library_name STRING,
context_library_version STRING,
context_referer STRING,
context_user_agent STRING,
event STRING,
event_text STRING,
location_info_advertising_zone STRING,
location_info_search_zip_code STRING,
location_info_user_zip_code STRING,
original_timestamp STRING,
results STRING,
search_for STRING,
search_params_filters_categories STRING,
search_params_filters_distance_from_provider STRING,
search_params_filters_first_name STRING,
search_params_filters_listing STRING,
search_params_filters_sp_features STRING,
search_params_page STRING,
search_params_query STRING,
search_params_search_experience STRING,
search_params_tokenized_query STRING,
search_params_type STRING,
sent_at STRING,
sort_sort_by STRING,
sort_sort_field STRING,
timestamp STRING,
total_pages STRING,
total_results STRING,
user_id STRING,
user_zip_code STRING,
query STRING,
search_type STRING,
uuid_ts STRING,
anonymous_id STRING,
context_page_path STRING,
context_page_title STRING,
context_page_url STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_SEGMENT}/angieslistweb_prod/incremental/daily/inc_angieslistweb_prod_searched_sp';