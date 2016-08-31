--  HIVE SCRIPT  : create_tf_sk_fact_web_metrics.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(sk_fact_web_metrics). 

--  Creating a DQ hive table(tf_sk_fact_web_metrics) over the incoming data
CREATE EXTERNAL TABLE  work_al_web_metrics.tf_sk_fact_web_metrics(
id STRING,
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
  '/user/hadoop/data/work/alwebmetrics/tf_sk_fact_web_metrics'
