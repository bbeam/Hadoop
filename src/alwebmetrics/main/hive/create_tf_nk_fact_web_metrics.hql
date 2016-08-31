--  HIVE SCRIPT  : create_tf_nk_fact_web_metrics.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_nk_fact_webmetrics). 

--  Creating a DQ hive table(tf_fact_web_metrics) over the incoming data
DROP TABLE IF EXISTS ${hivevar:WORK_AL_WEB_METRICS_DB}.tf_nk_fact_web_metrics;

CREATE EXTERNAL TABLE  ${hivevar:WORK_AL_WEB_METRICS_DB}.tf_nk_fact_web_metrics(
id STRING,
date_ak INT,
time_ak STRING,
legacy_spid INT,
new_world_spid INT,
source_ak  INT,
source_table   STRING,
member_id   INT,
user_id INT,
category_id INT,
event_type  STRING,
search_type STRING,
event_source TINYINT,
event_sub_source STRING,
search_text STRING,
qty INT)
PARTITIONED BY (event_type_key STRING)
LOCATION
  '${hivevar:WORK_DIR}/data/work/alwebmetrics/tf_nk_fact_web_metrics'
