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
        
pages_sel = FILTER pages BY edh_bus_date == '$EDH_BUS_DATE'
                                AND NOT (user_id matches '*.[a-zA-Z].*' );


/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
pages_check_user_id = FOREACH pages_sel GENERATE
                                    id AS id,
                                    est_sent_at AS est_sent_at,
                                    ( user_id IS NULL OR  user_id == '' ? $NUMERIC_MISSING_KEY  : ( INT ) user_id) AS user_id,
                                    ( user_id IS NULL OR  user_id == '' ? $NUMERIC_MISSING_KEY  :  NULL ) AS member_id;
                                
/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT pages_check_user_id INTO
                    pages_user_id_missing IF (member_id == $NUMERIC_MISSING_KEY ),
                    pages_user_id_available IF (member_id != $NUMERIC_MISSING_KEY );
                                
jn_pages_user_id_available_members = FOREACH (JOIN pages_user_id_available BY user_id LEFT , dim_members BY user_id ) 
                            GENERATE pages_user_id_available::id AS id,  
                                    pages_user_id_available::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_members::member_id) AS member_id;
                                    
/* Combine the 2 relations one with missing user_id and the other with user_id available */
un_pages_members = UNION jn_pages_user_id_available_members, pages_user_id_missing;


tf_segment_pages = FOREACH un_pages_members 
    GENERATE    id AS id, 
                (INT)(ToString(est_sent_at,'yyyyMMdd')) AS date_ak,
                ToString(est_sent_at,'HH:mm') AS time_ak,
                $NUMERIC_NA_KEY AS legacy_spid,
                $NUMERIC_NA_KEY AS new_world_spid,
                $NUMERIC_NA_KEY AS source_ak,
                '$STRING_NA_VALUE' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                $NUMERIC_NA_KEY AS category_id,
                '$TF_EVENT_NAME' AS event_type,
                '$STRING_NA_VALUE' AS search_type,
                $EVENT_SOURCE_WEB AS event_source:int,
                '$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
                ( CHARARRAY ) NULL AS search_text,
                1 AS qty,
                $TF_EVENT_KEY AS event_type_key;
     
STORE tf_segment_pages 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();