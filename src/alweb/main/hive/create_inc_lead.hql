--/*
--  HIVE SCRIPT  : create_inc_lead.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_lead). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/alweb/main/hive/create_inc_lead.hql \
						--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
						--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
						--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a incoming hive table(inc_lead) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_INCOMING_DB}.inc_lead
(
  lead_id STRING,
  title STRING,
  description STRING,
  urgency STRING,
  price STRING,
  max_quantity STRING,
  expired STRING,
  is_rejected STRING,
  rejection_reason STRING,
  user_id STRING,
  contact_information_id STRING,
  category_id STRING,
  postal_address_id STRING,
  version STRING,
  create_date STRING,
  create_by STRING,
  update_date STRING,
  update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/${hivevar:SOURCE_ALWEB}/fulfilment/full/daily/inc_lead';