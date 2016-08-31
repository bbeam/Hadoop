/*
PIG SCRIPT    : tf_fact_wm_14_segement_popular_category_tapped.pig
AUTHOR        : Abhijeet Purwar
DATE          : 30 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event popular_category_tapped from Segment Source
*/


pop_cat_tapped = 
        LOAD 'gold_sgmnt_events_gpap.dq_popular_category_tapped'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
pop_cat_tapped_fltr = FILTER pages BY edh_bus_date == '2016-01-01';

jn_pop_cat_tapped_fltr = FOREACH (JOIN pop_cat_tapped_fltr BY ((INT)user_id IS NULL ? -1 : (INT)user_id) LEFT , dim_members BY user_id ) 
                            GENERATE pop_cat_tapped_fltr::id AS id,  
                                    pop_cat_tapped_fltr::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;
                                    
tf_segment_pop_cat_tapped = FOREACH jn_pop_cat_tapped_fltr 
     GENERATE    id AS id, 
                (INT)(ToString(est_sent_at,'YYYYMMDD')) AS date_ak,
                ToString(est_sent_at,'hh:mm') AS time_ak,
                -2 AS legacy_spid,
                -2 AS new_world_spid,
                -2 AS source_ak,
                'Not Applicable' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                -2 AS category_id,
                'Popular Category Tapped' AS event_type,
                'Not Applicable' AS search_type,
                1 AS event_source:int,
                'android' AS event_sub_source,
                NULL AS search_text,
                1 AS qty,
                14 AS event_type_key;
     
STORE tf_segment_pop_cat_tapped 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();