--/*
--  HIVE SCRIPT  : create_inc_angieslistweb_prod_offer_page_loaded.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 5, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_angieslistweb_prod_offer_page_loaded). 

--*/

--  Creating a incoming hive table(inc_angieslistweb_prod_offer_page_loaded) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_angieslistweb_prod_offer_page_loaded
(
	id STRING,
	received_at STRING,
	uuid STRING,
	anonymous_id STRING,
	context_ip STRING,
	context_library_name STRING,
	context_library_version STRING,
	context_page_path STRING,
	context_page_referrer STRING,
	context_page_search STRING,
	context_page_title STRING,
	context_page_url STRING,
	context_user_agent STRING,
	description STRING,
	event STRING,
	event_text STRING,
	event_type STRING,
	original_timestamp STRING,
	sent_at STRING,
	service_provider_id STRING,
	sku_id STRING,
	sku_is_in_area STRING,
	sku_member_price_amount STRING,
	sku_member_price_currency STRING,
	sku_status STRING,
	sku_title STRING,
	timestamp STRING,
	user_id STRING,
	user_primary_ad_zone STRING,
	user_primary_zip_code STRING,
	user_selected_ad_zone STRING,
	user_selected_zip_code STRING,
	context_campaign_source STRING,
	context_campaign_name STRING,
	uuid_ts STRING,
	context_campaign_medium STRING,
	sku_category_name STRING,
	context_traits_alid STRING,
	context_traits_email STRING,
	context_traits_experiment_4_0_dpw_login_update STRING,
	context_traits_experiment_4_0_ecom_offer_detail_display_phone_number_45_sec_delay STRING,
	context_traits_experiment_4_0_login_spring_ecom STRING,
	context_traits_experiment_4_0_member_global_header_hide_search_bar_on_ecom_pages STRING,
	context_traits_experiment_4_0_member_global_header_hide_search_bar_on_ecom_pages_1 STRING,
	context_traits_experiment_ecom_checkout_cc_fields_hide_steps STRING,
	context_traits_experiment_ecom_checkout_cc_fields_hide_steps_1 STRING,
	context_traits_experiment_ecom_checkout_cc_fields_only_update STRING,
	context_traits_experiment_my_account_display_member_plan_and_notice STRING,
	context_traits_experiment_my_angie_popular_services_to_ecom STRING,
	context_traits_experiment_my_angie_popular_services_to_ecom_v2 STRING,
	context_traits_experiment_review_form_update_and_my_reviews_page STRING,
	context_traits_experiment_sp_profile_shop_tab_v2 STRING,
	context_traits_experiment_sp_profile_shop_tab_v2_1 STRING,
	context_traits_experiment_z100_review_form_update_and_my_reviews_page STRING,
	context_traits_first_name STRING,
	context_traits_last_name STRING,
	context_traits_experiment_sp_profile_ecom_link_or_shop_tab STRING,
	context_traits_experiment_launch_geosort STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/events/angieslistweb_prod/incremental/daily/inc_angieslistweb_prod_offer_page_loaded';