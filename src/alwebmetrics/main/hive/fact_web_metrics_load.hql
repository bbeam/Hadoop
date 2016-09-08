SET hive.exec.dynamic.partition.mode=non-strict;
-- ######################################################################################################### 
-- HIVE SCRIPT              :fact_web_metrics_load.hql
-- AUTHOR                   :Abhinav Mehar.
-- DESCRIPTION              :Hive to load the final fact_web_metrics table.
-- #########################################################################################################


use ${hivevar:GOLD_SHARED_DIM_DB};

-- =========Loading work table to target dimension table========
INSERT OVERWRITE TABLE ${hivevar:GOLD_AL_WEB_METRICS_DB}.fact_web_metrics
PARTITION(date_ak)
SELECT 
       id                  AS  id  ,
       time_ak             AS  time_ak ,
       legacy_spid         AS  legacy_spid ,
       new_world_spid      AS  new_world_spid  ,
       product_key         AS  product_key ,
       member_key          AS  member_key  ,
       category_key        AS  category_key    ,
       event_type_key      AS  event_type_key  ,
       source_ak           AS  source_ak   ,
       source_table        AS  source_table    ,
       category_id         AS  category_id ,
       member_id           AS  member_id   ,
       user_id             AS  user_id ,
       legacy_spid         AS  legacy_spid ,
       nw_spid             AS  nw_spid ,
       event_type          AS  event_type  ,
       search_type         AS  search_type ,
       event_source        AS  event_source    ,
       event_sub_source    AS  event_sub_source    ,
       search_text         AS  search_text ,
       qty                 AS  qty ,
	   date_ak             AS  date_ak 
FROM ${hivevar:WORK_AL_WEB_METRICS_DB}.tf_sk_fact_web_metrics;
