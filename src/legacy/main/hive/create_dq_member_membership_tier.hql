--/*
--  HIVE SCRIPT  : create_inc_member_membership_tier.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_member_membership_tier). 
--*/

--  Creating a incoming hive table(inc_member_membership_tier) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_member_membership_tier
(
	member_membership_tier_id INT,
	member_id INT,
	membership_tier_id INT,
	original_purchase_price DECIMAL(10,2),
	create_date TIMESTAMP,
	create_by STRING,
	update_date TIMESTAMP,
	update_by STRING,
	load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_member_membership_tier';
