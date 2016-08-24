*/--
--  HIVE SCRIPT  : create_tf_fact_web_metrics.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_fact_web_metrics). 

--*/

--  Creating a DQ hive table(tf_fact_web_metrics) over the incoming data
CREATE EXTERNAL TABLE  work_al_webmetrics.tf_fact_web_metrics(
date_ak int,
time_ak string,
legacy_spid int,
nw_spid int,
product_id  int,
product_table   string,
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
  '/user/hadoop/data/work/webmetrics/tf_fact_web_metrics'
