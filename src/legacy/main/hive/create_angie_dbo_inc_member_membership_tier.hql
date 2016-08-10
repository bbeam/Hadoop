--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_member_membership_tier.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_member_membership_tier). 

--*/ 

--  Creating a incoming hive table(inc_member_membership_tier) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_member_membership_tier
(
	member_membership_tier_id STRING,
	member_id STRING,
	membership_tier_id STRING,
	original_purchase_price STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING,
	green_thunder_membership_tier_id STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/full/daily/inc_member_membership_tier';