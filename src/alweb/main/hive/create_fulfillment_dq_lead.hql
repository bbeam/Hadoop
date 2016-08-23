--/*
--  HIVE SCRIPT  : create_fulfillment_dq_lead.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_lead). 
--*/

--  Creating a dq hive table(dq_lead) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_lead
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
  est_create_date TIMESTAMP, 
  create_date TIMESTAMP,
  create_by INT,
  est_update_date TIMESTAMP,
  update_date TIMESTAMP,
  update_by INT,
  est_load_timestamp TIMESTAMP,
  utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/alweb/fulfillment/full/daily/dq_lead';