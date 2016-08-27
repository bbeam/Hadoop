--  HIVE SCRIPT  : create_tf_nk_fact_webmetrics.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_nk_fact_webmetrics). 

--  Creating a DQ hive table(tf_fact_web_metrics) over the incoming data
DROP TABLE IF EXISTS ${hivevar:WORK_AL_WEBMETRICS_DB}.tf_nk_fact_webmetrics;

CREATE EXTERNAL TABLE  ${hivevar:WORK_AL_WEBMETRICS_DB}.tf_nk_fact_webmetrics(
date_ak int,
time_ak string,
legacy_spid int,
new_world_spid int,
source_ak  int,
source_table   string,
member_id   int,
user_id int,
category_id int,
event_type  string,
search_type string,
event_source tinyint,
event_sub_source string,
search_text string,
qty int)
PARTITIONED BY (event_type_key STRING)
LOCATION
  '${hivevar:WORK_DIR}/data/work/alwebmetrics/tf_nk_fact_webmetrics'
