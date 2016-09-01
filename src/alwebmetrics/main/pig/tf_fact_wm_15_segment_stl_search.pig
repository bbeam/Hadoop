/*
PIG SCRIPT    : tf_fact_wm_15_segement_stl_search.pig
AUTHOR        : Abhijeet Purwar
DATE          : 30 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event stl_search from Segment Source
*/


stl_search = 
        LOAD '$GOLD_SEGMENT_EVENTS_GPAP_DB.dq_stl_search'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
dim_members =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

stl_search_filtered = FILTER stl_search BY edh_bus_date == '$EDH_BUS_DATE';

/* Check if user_id is null. If user_id  is null then populate both member_id and user_id as missing */
stl_search_filtered_mark_check_user_id = FOREACH stl_search_filtered GENERATE
                                (chararray)id AS id,
                                est_sent_at AS est_sent_at,
                                (user_id IS NULL OR user_id =='' ? $NUMERIC_MISSING_KEY : (int) user_id) AS user_id,
                                (user_id IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS member_id;

/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT stl_search_filtered_mark_check_user_id INTO
                    stl_search_user_id_missing IF (user_id == $NUMERIC_MISSING_KEY),
                    stl_search_user_id_available IF (user_id != $NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_stl_search_user_id_available_members = FOREACH (JOIN stl_search_user_id_available BY user_id LEFT , dim_members BY user_id )
                         GENERATE   stl_search_user_id_available::id AS id,
                                    stl_search_user_id_available::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_members::member_id) AS member_id;

/* Combine the 2 relations one with missing user_id and the other with user_id available */
un_stl_search_members = UNION jn_stl_search_user_id_available_members, stl_search_user_id_missing;

/* Format the record as per the Target Table structure */
tf_segment_stl_search = FOREACH un_stl_search_members 
     GENERATE    id AS id, 
                (INT)(ToString(est_sent_at,'YYYYMMDD')) AS date_ak,
                ToString(est_sent_at,'hh:mm') AS time_ak,
                $NUMERIC_NA_KEY AS legacy_spid,
                $NUMERIC_NA_KEY AS new_world_spid,
                $NUMERIC_NA_KEY AS source_ak,
                '$STRING_NA_VALUE' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                $NUMERIC_NA_KEY AS category_id,
                '$TF_EVENT_NAME' AS event_type,
                '$TF_SEARCH_NAME' AS search_type,
                $EVENT_SOURCE_IOS AS event_source:int,
                (chararray)'$EVENT_SUB_SOURCE_ANDROID' AS event_sub_source:chararray,
                (chararray) NULL AS search_text,
                $TF_STL_SEARCH_QTY AS qty,
                $TF_EVENT_KEY AS event_type_key;

STORE tf_segment_stl_search 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();                              