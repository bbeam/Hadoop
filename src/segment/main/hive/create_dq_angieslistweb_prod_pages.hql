--  HIVE SCRIPT  : create_dq_angieslistweb_prod_pages.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 5, 2016
--  DESCRIPTION  : Creation of hive dq table(dq_angieslistweb_prod_pages). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_dq_angieslistweb_prod_pages.hql \
--     --hivevar SEGMENT_GOLD_DB="${SEGMENT_GOLD_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 

--*/

--  Creating a DQ hive table(Dq_alwp_pages) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_GOLD_DB}.dq_angieslistweb_prod_pages
( 
	id VARCHAR(254),
	received_at TIMESTAMP,
	uuid BIGINT,
	anonymous_id VARCHAR(256),
	context_ip VARCHAR(256),
	context_library_name VARCHAR(256),
	context_library_version VARCHAR(256),
	context_page_path VARCHAR(256),
	context_page_referrer VARCHAR(256),
	context_page_search VARCHAR(256),
	context_page_title VARCHAR(256),
	context_page_url VARCHAR(256),
	context_user_agent VARCHAR(256),
	name VARCHAR(256),
	original_timestamp TIMESTAMP,
	path VARCHAR(256),
	properties_title VARCHAR(256),
	properties_url VARCHAR(256),
	referrer VARCHAR(256),
	search VARCHAR(256),
	sent_at TIMESTAMP,
	timestamp TIMESTAMP,
	title VARCHAR(256),
	type VARCHAR(256),
	url VARCHAR(256),
	user_id VARCHAR(256),
	context_campaign_term VARCHAR(256),
	context_campaign_source VARCHAR(256),
	s_kwcid VARCHAR(256),
	cid VARCHAR(256),
	context_campaign_name VARCHAR(256),
	uuid_ts TIMESTAMP,
	context_campaign_medium VARCHAR(256),
	test_prop VARCHAR(256),
	context_campaign_content VARCHAR(256),
	category VARCHAR(256),
	free_market boolean,
	market_id BIGINT,
	market_name VARCHAR(256),
	market_zip BIGINT,
	member_type VARCHAR(256),
	payment_type VARCHAR(256),
	plan_name VARCHAR(256),
	pricing VARCHAR(256),
	context_traits_alid BIGINT,
	context_traits_email VARCHAR(256),
	context_traits_experiment_4_0_dpw_login_update VARCHAR(256),
	context_traits_experiment_4_0_ecom_offer_detail_display_phone_number_45_sec_delay VARCHAR(256),
	context_traits_experiment_4_0_login_spring_ecom VARCHAR(256),
	context_traits_experiment_4_0_member_global_header_hide_search_bar_on_ecom_pages VARCHAR(256),
	context_traits_experiment_4_0_member_global_header_hide_search_bar_on_ecom_pages_1 VARCHAR(256),
	context_traits_experiment_ecom_checkout_cc_fields_hide_steps VARCHAR(256),
	context_traits_experiment_ecom_checkout_cc_fields_hide_steps_1 VARCHAR(256),
	context_traits_experiment_ecom_checkout_cc_fields_only_update VARCHAR(256),
	context_traits_experiment_my_account_display_member_plan_and_notice VARCHAR(256),
	context_traits_experiment_my_angie_popular_services_to_ecom VARCHAR(256),
	context_traits_experiment_my_angie_popular_services_to_ecom_v2 VARCHAR(256),
	context_traits_experiment_review_form_update_and_my_reviews_page VARCHAR(256),
	context_traits_experiment_sp_profile_ecom_link_or_shop_tab VARCHAR(256),
	context_traits_experiment_sp_profile_shop_tab_v2 VARCHAR(256),
	context_traits_experiment_sp_profile_shop_tab_v2_1 VARCHAR(256),
	context_traits_experiment_z100_review_form_update_and_my_reviews_page VARCHAR(256),
	context_traits_first_name VARCHAR(256),
	context_traits_last_name VARCHAR(256),
	context_traits_experiment_geosort_round_2 VARCHAR(256),
	context_traits_experiment_launch_geosort VARCHAR(256),
	context_traits_experiment_search_results_hide_description_and_review_counts_for_non_advertisers VARCHAR(256),
	context_traits_experiment_z100_search_results_hide_description_and_review_counts VARCHAR(256),
	load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_SEGMENT}/angieslistweb_prod/incremental/daily/dq_angieslistweb_prod_pages';

