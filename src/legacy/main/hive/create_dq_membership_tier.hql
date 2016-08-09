--/*
--  HIVE SCRIPT  : create_dq_membership_tier.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 8, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.Membership_Tier) 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_membership_tier
(
	membership_tier_id INT,
	membership_tier_name STRING,
	membership_tier_description STRING,
	is_classic_vertical_enabled TINYINT,
	is_health_vertical_enabled TINYINT,
	is_classic_car_vertical_enabled TINYINT,
	is_online_access_enabled TINYINT,
	is_print_magazine_enabled TINYINT,
	is_national_search_enabled TINYINT,
	is_online_customer_care_enabled TINYINT,
	is_call_center_customer_care_enabled TINYINT,
	is_complaint_resolution_enabled TINYINT,
	is_priority_customer_care_enabled TINYINT,
	is_home_tuneup_enabled TINYINT,
	is_digital_magazine_enabled TINYINT,
	is_active_for_purchase_enabled TINYINT,
	term_id INT,
	create_date  TIMESTAMP,
	create_by STRING,
	update_date  TIMESTAMP,
	update_by STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_membership_tier';
