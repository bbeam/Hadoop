--/*
--  HIVE SCRIPT  : create_dq_lead.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_lead). 
--  USAGE		 : hive -f ${S3_BUCKET}/src/alweb/main/hive/create_dq_lead.hql \
						--hivevar ALWEB_INCOMING_DB="${ALWEB_INCOMING_DB}" \
						--hivevar SOURCE_ALWEB="${SOURCE_ALWEB}" \
						--hivevar S3_BUCKET="${S3_BUCKET}"
--*/

--  Creating a dq hive table(dq_lead) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_lead
(
  lead_id INT,
  title VARCHAR(254),
  description VARCHAR(2048),
  urgency VARCHAR(50),
  price DECIMAL(10,2),
  max_quantity INT,
  expired TINYINT,
  is_rejected TINYINT,
  rejection_reason STRING,
  user_id INT,
  contact_information_id INT,
  category_id INT,
  postal_address_id INT,
  version INT,
  create_date TIMESTAMP,
  create_by INT,
  update_date TIMESTAMP,
  update_by INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/fulfilment/full/daily/inc_lead';