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
        
pages_sel = FILTER pages BY edh_bus_date == '2016-01-01'
                                AND NOT (user_id matches '*.[a-zA-Z].*' );


/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
pages_check_user_id = FOREACH pages_sel GENERATE
                                    id AS id,
                                    est_sent_at AS est_sent_at,
                                    ( user_id IS NULL OR  user_id == '' ? -1 : ( INT ) user_id) AS user_id,
                                    ( user_id IS NULL OR  user_id == '' ? -1 :  NULL ) AS member_id;
                                
/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT pages_check_user_id INTO
                    pages_user_id_missing IF (member_id == -1),
                    pages_user_id_available IF (member_id != -1);
                                
jn_pages_user_id_available_members = FOREACH (JOIN pages_user_id_available BY user_id LEFT , dim_members BY user_id ) 
                            GENERATE pages_user_id_available::id AS id,  
                                    pages_user_id_available::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;
                                    
/* Combine the 2 relations one with missing user_id and the other with user_id available */
un_pages_members = UNION jn_pages_user_id_available_members, pages_user_id_missing;


tf_segment_pages = FOREACH un_pages_members 
    GENERATE    id AS id, 
                (INT)(ToString(est_sent_at,'yyyyMMdd')) AS date_ak,
                ToString(est_sent_at,'HH:mm') AS time_ak,
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
                ( CHARARRAY ) NULL AS search_text,
                1 AS qty,
                230 AS event_type_key;
     
STORE tf_segment_pages 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();