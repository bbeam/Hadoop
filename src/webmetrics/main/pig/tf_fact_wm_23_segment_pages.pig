/*
PIG SCRIPT    : tf_fact_wm_segement_pages.pig
AUTHOR        : Anil Aleppy
DATE          : 27 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Pages from Segment Source
*/


pages = 
        LOAD 'gold_sgmnt_events_alwp.dq_pages'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
pages_filtered = FILTER pages BY edh_bus_date == '2016-01-01'
                                AND NOT (user_id matches '*.[a-zA-Z].*' );


jn_pages_filtered_members = FOREACH (JOIN pages_filtered BY ((INT)user_id IS NULL ? -1 : (INT)user_id) LEFT , dim_members BY user_id ) 
                            GENERATE pages_filtered::id AS id,  
                                    pages_filtered::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;


                                    

tf_segment_pages = FOREACH jn_pages_filtered_members 
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
                'Web Segment Page Load' AS event_type,
                'Not Applicable' AS search_type,
                0 AS event_source:int,
                'web' AS event_sub_source,
                NULL AS search_text,
                1 AS qty,
                23 AS event_type_key;
     
STORE tf_segment_pages 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();