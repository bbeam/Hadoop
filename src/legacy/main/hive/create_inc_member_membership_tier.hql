--/*
--  HIVE SCRIPT  : create_inc_member_membership_tier.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_member_membership_tier). 
--  USAGE 		 : hive -f s3://al-edh-dev/src/legacy/main/hive/create_inc_member_membership_tier.hql \
--					--hivevar LEGACY_INCOMING_DB="${LEGACY_INCOMING_DB}" \
--					--hivevar S3_BUCKET="${S3_BUCKET}" \
--					--hivevar SOURCE_LEGACY="${SOURCE_LEGACY}"
--*/ 

--  Creating a incoming hive table(inc_member_membership_tier) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_INCOMING_DB}.inc_member_membership_tier
(
	member_membership_tier_id STRING,
	member_id STRING,
	membership_tier_id STRING,
	original_purchase_price STRING,
	create_date STRING,
	create_by STRING,
	update_date STRING,
	update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_LEGACY}/angie/full/daily/inc_member_membership_tier';