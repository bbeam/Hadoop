/*#########################################################################################################
PIG SCRIPT                              :sk_fact_web_metrics_load.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load sk_fact_web_metrics.
#########################################################################################################*/

table_tf_fact_web_metrics=
        LOAD '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_market=
        LOAD '$GOLD_SHARED_DIM_DB.dim_market'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_product=
        LOAD '$GOLD_SHARED_DIM_DB.dim_product'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        
        
table_dim_event_type=
        LOAD '$GOLD_AL_WEB_METRICS_DB.dim_event_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_category=
        LOAD '$GOLD_SHARED_DIM_DB.dim_category'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dim_service_provider=
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_date = 
        LOAD '$GOLD_SHARED_DIM_DB.dim_date'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
		
table_dim_time=
        LOAD '$GOLD_SHARED_DIM_DB.dim_time'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

        
jn_tf_fwb_wt_prod = FOREACH (JOIN table_tf_fact_web_metrics BY (source_ak,source_table) LEFT,table_dim_product BY (source_ak,source_table)) 
                           GENERATE (table_dim_product::product_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_product::product_key) AS  product_key,
						            table_tf_fact_web_metrics::event_type_key  AS  event_type_key,
						            table_tf_fact_web_metrics::id              AS  id,
									table_tf_fact_web_metrics::date_ak         AS  date_ak,
									table_tf_fact_web_metrics::time_ak         AS  time_ak,
					                table_tf_fact_web_metrics::source_ak       AS  source_ak,
                                    table_tf_fact_web_metrics::source_table    AS  source_table,
                                    table_tf_fact_web_metrics::category_id     AS  category_id,
                                    table_tf_fact_web_metrics::member_id       AS  member_id,
                                    table_tf_fact_web_metrics::user_id         AS  user_id,
                                    table_tf_fact_web_metrics::legacy_spid     AS  legacy_spid,
                                    table_tf_fact_web_metrics::new_world_spid  AS  new_world_spid,
                                    table_tf_fact_web_metrics::event_type      AS  event_type,
                                    table_tf_fact_web_metrics::search_type     AS  search_type,
                                    table_tf_fact_web_metrics::event_source    AS  event_source,
                                    table_tf_fact_web_metrics::event_sub_source    AS  event_sub_source,
                                    table_tf_fact_web_metrics::search_text     AS  search_text,
                                    table_tf_fact_web_metrics::qty             AS  qty
									;


jn_tf_fwb_wt_ctgy = FOREACH ( JOIN jn_tf_fwb_wt_prod BY (category_id) LEFT,table_dim_category BY (category_id))
                GENERATE            (table_dim_category::category_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_category::category_key) AS  category_key,
						            jn_tf_fwb_wt_prod::product_key     AS  product_key,
						            jn_tf_fwb_wt_prod::event_type_key  AS  event_type_key,									
						            jn_tf_fwb_wt_prod::id              AS  id,
									jn_tf_fwb_wt_prod::date_ak         AS  date_ak,
									jn_tf_fwb_wt_prod::time_ak         AS  time_ak,									
						            jn_tf_fwb_wt_prod::source_ak       AS  source_ak,
                                    jn_tf_fwb_wt_prod::source_table    AS  source_table,
                                    jn_tf_fwb_wt_prod::category_id     AS  category_id,
                                    jn_tf_fwb_wt_prod::member_id       AS  member_id,
                                    jn_tf_fwb_wt_prod::user_id         AS  user_id,
                                    jn_tf_fwb_wt_prod::legacy_spid     AS  legacy_spid,
                                    jn_tf_fwb_wt_prod::new_world_spid  AS  new_world_spid,
                                    jn_tf_fwb_wt_prod::event_type      AS  event_type,
                                    jn_tf_fwb_wt_prod::search_type     AS  search_type,
                                    jn_tf_fwb_wt_prod::event_source    AS  event_source,
                                    jn_tf_fwb_wt_prod::event_sub_source    AS  event_sub_source,
                                    jn_tf_fwb_wt_prod::search_text     AS  search_text,
                                    jn_tf_fwb_wt_prod::qty             AS  qty
									;

								
jn_tf_fwb_wt_mbr = FOREACH ( JOIN jn_tf_fwb_wt_ctgy BY (member_id,user_id) LEFT,table_dim_member BY (member_id,user_id))
                GENERATE            (table_dim_member::member_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_member::member_key) AS  member_key,
				                    jn_tf_fwb_wt_ctgy::category_key    AS  category_key,
						            jn_tf_fwb_wt_ctgy::product_key     AS  product_key,
						            jn_tf_fwb_wt_ctgy::event_type_key  AS  event_type_key,																		
						            jn_tf_fwb_wt_ctgy::id              AS  id,
									jn_tf_fwb_wt_ctgy::date_ak         AS  date_ak,
									jn_tf_fwb_wt_ctgy::time_ak         AS  time_ak,																		
						            jn_tf_fwb_wt_ctgy::source_ak       AS  source_ak,
                                    jn_tf_fwb_wt_ctgy::source_table    AS  source_table,
                                    jn_tf_fwb_wt_ctgy::category_id     AS  category_id,
                                    jn_tf_fwb_wt_ctgy::member_id       AS  member_id,
                                    jn_tf_fwb_wt_ctgy::user_id         AS  user_id,
                                    jn_tf_fwb_wt_ctgy::legacy_spid     AS  legacy_spid,
                                    jn_tf_fwb_wt_ctgy::new_world_spid  AS  new_world_spid,
                                    jn_tf_fwb_wt_ctgy::event_type      AS  event_type,
                                    jn_tf_fwb_wt_ctgy::search_type     AS  search_type,
                                    jn_tf_fwb_wt_ctgy::event_source    AS  event_source,
                                    jn_tf_fwb_wt_ctgy::event_sub_source    AS  event_sub_source,
                                    jn_tf_fwb_wt_ctgy::search_text     AS  search_text,
                                    jn_tf_fwb_wt_ctgy::qty             AS  qty
;

jn_tf_fwb_wt_sp = FOREACH ( JOIN jn_tf_fwb_wt_mbr BY (legacy_spid,new_world_spid) LEFT,table_dim_service_provider BY (legacy_spid,new_world_spid))
                GENERATE            (table_dim_service_provider::service_provider_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_service_provider::service_provider_key) AS service_provider_key,
				                    jn_tf_fwb_wt_mbr::member_key      AS  member_key,
				                    jn_tf_fwb_wt_mbr::category_key    AS  category_key,
						            jn_tf_fwb_wt_mbr::product_key     AS  product_key,
						            jn_tf_fwb_wt_mbr::event_type_key  AS  event_type_key,									
						            jn_tf_fwb_wt_mbr::id              AS  id,
									jn_tf_fwb_wt_mbr::date_ak         AS  date_ak,
									jn_tf_fwb_wt_mbr::time_ak         AS  time_ak,																										
						            jn_tf_fwb_wt_mbr::source_ak       AS  source_ak,
                                    jn_tf_fwb_wt_mbr::source_table    AS  source_table,
                                    jn_tf_fwb_wt_mbr::category_id     AS  category_id,
                                    jn_tf_fwb_wt_mbr::member_id       AS  member_id,
                                    jn_tf_fwb_wt_mbr::user_id         AS  user_id,
                                    jn_tf_fwb_wt_mbr::legacy_spid     AS  legacy_spid,
                                    jn_tf_fwb_wt_mbr::new_world_spid  AS  new_world_spid,
                                    jn_tf_fwb_wt_mbr::event_type      AS  event_type,
                                    jn_tf_fwb_wt_mbr::search_type     AS  search_type,
                                    jn_tf_fwb_wt_mbr::event_source    AS  event_source,
                                    jn_tf_fwb_wt_mbr::event_sub_source    AS  event_sub_source,
                                    jn_tf_fwb_wt_mbr::search_text     AS  search_text,
                                    jn_tf_fwb_wt_mbr::qty             AS  qty
;

jn_tf_fwb_wt_date = FOREACH ( JOIN jn_tf_fwb_wt_sp BY (date_ak) LEFT,table_dim_date BY (date_ak))
                GENERATE            (table_dim_date::date_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_date::date_key) AS date_key,
				                    jn_tf_fwb_wt_sp::service_provider_key AS service_provider_key,
				                    jn_tf_fwb_wt_sp::member_key      AS  member_key,
				                    jn_tf_fwb_wt_sp::category_key    AS  category_key,
						            jn_tf_fwb_wt_sp::product_key     AS  product_key,
						            jn_tf_fwb_wt_sp::event_type_key  AS  event_type_key,																		
						            jn_tf_fwb_wt_sp::id              AS  id,
									jn_tf_fwb_wt_sp::date_ak         AS  date_ak,
									jn_tf_fwb_wt_sp::time_ak         AS  time_ak,
						            jn_tf_fwb_wt_sp::source_ak       AS  source_ak,
                                    jn_tf_fwb_wt_sp::source_table    AS  source_table,
                                    jn_tf_fwb_wt_sp::category_id     AS  category_id,
                                    jn_tf_fwb_wt_sp::member_id       AS  member_id,
                                    jn_tf_fwb_wt_sp::user_id         AS  user_id,
                                    jn_tf_fwb_wt_sp::legacy_spid     AS  legacy_spid,
                                    jn_tf_fwb_wt_sp::new_world_spid  AS  new_world_spid,
                                    jn_tf_fwb_wt_sp::event_type      AS  event_type,
                                    jn_tf_fwb_wt_sp::search_type     AS  search_type,
                                    jn_tf_fwb_wt_sp::event_source    AS  event_source,
                                    jn_tf_fwb_wt_sp::event_sub_source    AS  event_sub_source,
                                    jn_tf_fwb_wt_sp::search_text     AS  search_text,
                                    jn_tf_fwb_wt_sp::qty             AS  qty
;


jn_tf_fwb_wt_time = FOREACH ( JOIN jn_tf_fwb_wt_date BY (time_ak) LEFT,table_dim_time BY (time_ak))
                GENERATE            jn_tf_fwb_wt_date::id              AS  id,									
				                    jn_tf_fwb_wt_date::date_key AS date_key,
				                    (table_dim_time::time_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_time::time_key) AS time_key,
				                    jn_tf_fwb_wt_date::service_provider_key AS service_provider_key,
						            jn_tf_fwb_wt_date::product_key     AS  product_key,
				                    jn_tf_fwb_wt_date::member_key      AS  member_key,
				                    jn_tf_fwb_wt_date::category_key    AS  category_key,
						            jn_tf_fwb_wt_date::event_type_key  AS  event_type_key,																		
						            jn_tf_fwb_wt_date::source_ak       AS  source_ak,
                                    jn_tf_fwb_wt_date::source_table    AS  source_table,
                                    jn_tf_fwb_wt_date::category_id     AS  category_id,
                                    jn_tf_fwb_wt_date::member_id       AS  member_id,
                                    jn_tf_fwb_wt_date::user_id         AS  user_id,
                                    jn_tf_fwb_wt_date::legacy_spid     AS  legacy_spid,
                                    jn_tf_fwb_wt_date::new_world_spid  AS  new_world_spid,
                                    jn_tf_fwb_wt_date::event_type      AS  event_type,
                                    jn_tf_fwb_wt_date::search_type     AS  search_type,
                                    jn_tf_fwb_wt_date::event_source    AS  event_source,
                                    jn_tf_fwb_wt_date::event_sub_source    AS  event_sub_source,
                                    jn_tf_fwb_wt_date::search_text     AS  search_text,
                                    jn_tf_fwb_wt_date::qty             AS  qty
;

jn_tf_fwb_wt_event= FOREACH ( JOIN jn_tf_fwb_wt_time BY (event_type,search_type,event_source,event_sub_source) LEFT,table_dim_event_type BY (event_type,search_type,event_source,event_sub_source))
                GENERATE            jn_tf_fwb_wt_time::id              AS  id,									
				                    jn_tf_fwb_wt_time::date_key        AS date_key,
				                    jn_tf_fwb_wt_time::time_key        AS time_key,
				                    jn_tf_fwb_wt_time::service_provider_key AS service_provider_key,
						            jn_tf_fwb_wt_time::product_key     AS  product_key,
				                    jn_tf_fwb_wt_time::member_key      AS  member_key,
				                    jn_tf_fwb_wt_time::category_key    AS  category_key,
				                    (table_dim_event_type::event_type_key IS NULL?(INT)$NUMERIC_UNKOWN_KEY: table_dim_event_type::event_type_key) AS event_type_key,
						            jn_tf_fwb_wt_time::source_ak       AS  source_ak,
                                    jn_tf_fwb_wt_time::source_table    AS  source_table,
                                    jn_tf_fwb_wt_time::category_id     AS  category_id,
                                    jn_tf_fwb_wt_time::member_id       AS  member_id,
                                    jn_tf_fwb_wt_time::user_id         AS  user_id,
                                    jn_tf_fwb_wt_time::legacy_spid     AS  legacy_spid,
                                    jn_tf_fwb_wt_time::new_world_spid  AS  new_world_spid,
                                    jn_tf_fwb_wt_time::event_type      AS  event_type,
                                    jn_tf_fwb_wt_time::search_type     AS  search_type,
                                    jn_tf_fwb_wt_time::event_source    AS  event_source,
                                    jn_tf_fwb_wt_time::event_sub_source    AS  event_sub_source,
                                    jn_tf_fwb_wt_time::search_text     AS  search_text,
                                    jn_tf_fwb_wt_time::qty             AS  qty
;



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

STORE jn_tf_fwb_wt_time 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_sk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();