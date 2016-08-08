--  HIVE SCRIPT  : create_dq_angieslistweb_prod_offer_page_loaded.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 5, 2016
--  DESCRIPTION  : Creation of hive dq table(dq_angieslistweb_prod_offer_page_loaded). 
--  USAGE    : hive -f s3://al-edh-dev/src/segment/main/hive/create_dq_angieslistweb_prod_offer_page_loaded.hql \
--     --hivevar SEGMENT_GOLD_DB="${SEGMENT_GOLD_DB}" \
--     --hivevar S3_BUCKET="${S3_BUCKET}" \
--     --hivevar SOURCE_SEGMENT="${SOURCE_SEGMENT}" 

--*/

--  Creating a DQ hive table(Dq_alwp_offer_page_loaded) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_GOLD_DB}.dq_angieslistweb_prod_offer_page_loaded
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
	description VARCHAR(256),
	event VARCHAR(256),
	event_text VARCHAR(256),
	event_type VARCHAR(256),
	original_timestamp TIMESTAMP,
	sent_at TIMESTAMP,
	service_provider_id VARCHAR(256),
	sku_id VARCHAR(256),
	sku_is_in_area BOOLEAN,
	sku_member_price_amount BIGINT,
	sku_member_price_currency VARCHAR(256),
	sku_status VARCHAR(256),
	sku_title VARCHAR(256),
	timestamp TIMESTAMP,
	user_id VARCHAR(256),
	user_primary_ad_zone BIGINT,
	user_primary_zip_code VARCHAR(256),
	user_selected_ad_zone BIGINT,
	user_selected_zip_code VARCHAR(256),
	context_campaign_source VARCHAR(256),
	context_campaign_name VARCHAR(256),
	uuid_ts TIMESTAMP,
	context_campaign_medium VARCHAR(256),
	sku_category_name VARCHAR(256),
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
	context_traits_experiment_sp_profile_shop_tab_v2 VARCHAR(256),
	context_traits_experiment_sp_profile_shop_tab_v2_1 VARCHAR(256),
	context_traits_experiment_z100_review_form_update_and_my_reviews_page VARCHAR(256),
	context_traits_first_name VARCHAR(256),
	context_traits_last_name VARCHAR(256),
	context_traits_experiment_sp_profile_ecom_link_or_shop_tab VARCHAR(256),
	context_traits_experiment_launch_geosort VARCHAR(256),
	load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_SEGMENT}/angieslistweb_prod/incremental/daily/dq_angieslistweb_prod_offer_page_loaded';

