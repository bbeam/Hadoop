--  HIVE SCRIPT  : create_tf_sk_fact_webmetrics.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_sk_fact_webmetrics). 

--  Creating a DQ hive table(tf_sk_fact_webmetrics) over the incoming data
DROP TABLE IF EXISTS ${hivevar:WORK_AL_WEBMETRICS_DB}.tf_sk_fact_webmetrics;

CREATE EXTERNAL TABLE  ${hivevar:WORK_AL_WEBMETRICS_DB}.tf_sk_fact_webmetrics(
date_key    INT,
time_key    INT,
service_provider_key    INT,
product_key INT,
member_key  INT,
category_key    INT,
event_type_key  INT,
source_ak   INT,
source_table    STRING,
category_id INT,
member_id   INT,
user_id INT,
legacy_spid INT,
new_world_spid  INT,
event_type  STRING,
search_type STRING,
event_source    TINYINT,
event_sub_source    STRING,
search_text STRING,
qty INT
)
LOCATION
  '${hivevar:WORK_DIR}/data/work/alwebmetrics/tf_sk_fact_webmetrics'
