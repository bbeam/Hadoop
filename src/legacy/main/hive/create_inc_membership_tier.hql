--/*
--  HIVE SCRIPT  : create_inc_membership_tier.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 8, 2016
--  DESCRIPTION  : Creation of hive incoming table(angie.Membership_Tier) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_membership_tier
(
	membership_tier_id STRING,
	membership_tier_name STRING,
	membership_tier_description STRING,
	is_classic_vertical_enabled STRING,
	is_health_vertical_enabled STRING,
	is_classic_car_vertical_enabled STRING,
	is_online_access_enabled STRING,
	is_print_magazine_enabled STRING,
	is_national_search_enabled STRING,
	is_online_customer_care_enabled STRING,
	is_call_center_customer_care_enabled STRING,
	is_complaint_resolution_enabled STRING,
	is_priority_customer_care_enabled STRING,
	is_home_tuneup_enabled STRING,
	is_digital_magazine_enabled STRING,
	is_active_for_purchase_enabled STRING,
	term_id STRING,
	create_date  STRING,
	create_by STRING,
	update_date  STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_membership_tier';
