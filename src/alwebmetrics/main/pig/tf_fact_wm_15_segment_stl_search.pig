/*
PIG SCRIPT    : tf_fact_wm_15_segement_stl_search.pig
AUTHOR        : Abhijeet Purwar
DATE          : 30 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event stl_search from Segment Source
*/



stl_search = 
        LOAD 'gold_sgmnt_events_gpap.dq_stl_search'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
stl_search_fltr = FILTER stl_search BY edh_bus_date == '2016-07-01';

jn_stl_search_fltr = FOREACH (JOIN stl_search_fltr BY ((INT)user_id IS NULL ? -1 : (INT)user_id) LEFT , dim_members BY user_id ) 
                            GENERATE stl_search_fltr::id AS id,  
                                    stl_search_fltr::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;

                                    
tf_segment_stl_search = FOREACH jn_stl_search_fltr 
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
                'STL Search' AS event_type,
                'Search The List' AS search_type,
                1 AS event_source:int,
                'android' AS event_sub_source,
                NULL AS search_text,
                1 AS qty,
                15 AS event_type_key;
                
/*

tf_segment_stl_search = LIMIT tf_segment_stl_search 10;
dump tf_segment_stl_search;

tf_segment_stl_search_group = GROUP tf_segment_stl_search ALL;
counted = FOREACH tf_segment_stl_search_group GENERATE COUNT(tf_segment_stl_search);
dump counted;
*/

     
STORE tf_segment_stl_search 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();                              