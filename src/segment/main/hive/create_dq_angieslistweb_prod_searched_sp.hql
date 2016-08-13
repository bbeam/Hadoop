--  HIVE SCRIPT  : create_dq_angieslistweb_prod_searched_sp.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(dq_searched_sp). 

--*/

--  Creating a DQ hive table(dq_searched_sp) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_searched_sp
(
id  VARCHAR(254),
est_received_at TIMESTAMP,
received_at TIMESTAMP,
uuid    BIGINT,
advertisers_on_page BIGINT,
context_auth_token  VARCHAR(256),
context_ip  VARCHAR(256),
context_library_name    VARCHAR(256),
context_library_version VARCHAR(256),
context_referer VARCHAR(256),
context_user_agent  VARCHAR(256),
event   VARCHAR(256),
event_text  VARCHAR(256),
location_info_advertising_zone  BIGINT,
location_info_search_zip_code   VARCHAR(256),
location_info_user_zip_code VARCHAR(256),
est_original_timestamp  TIMESTAMP,
original_timestamp  TIMESTAMP,
results VARCHAR(256),
search_for  VARCHAR(256),
search_params_filters_categories    VARCHAR(256),
search_params_filters_distance_from_provider    BIGINT,
search_params_filters_first_name    VARCHAR(256),
search_params_filters_listing   BIGINT,
search_params_filters_sp_features   VARCHAR(256),
search_params_page  BIGINT,
search_params_query VARCHAR(256),
search_params_search_experience VARCHAR(256),
search_params_tokenized_query   VARCHAR(256),
search_params_type  VARCHAR(256),
est_sent_at TIMESTAMP,
sent_at TIMESTAMP,
sort_sort_by    VARCHAR(256),
sort_sort_field VARCHAR(256),
est_timestamp   TIMESTAMP,
timestamp   TIMESTAMP,
total_pages BIGINT,
total_results   BIGINT,
user_id VARCHAR(256),
user_zip_code   VARCHAR(256),
query   VARCHAR(256),
search_type VARCHAR(256),
est_uuid_ts TIMESTAMP,
uuid_ts TIMESTAMP,
anonymous_id    VARCHAR(256),
context_page_path   VARCHAR(256),
context_page_title  VARCHAR(256),
context_page_url    VARCHAR(256),
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/segment/events/angieslistweb_prod/incremental/daily/dq_searched_sp';

