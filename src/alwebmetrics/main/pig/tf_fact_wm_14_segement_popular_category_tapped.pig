/*
PIG SCRIPT    : tf_fact_wm_14_segement_popular_category_tapped.pig
AUTHOR        : Abhijeet Purwar
DATE          : 30 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event popular_category_tapped from Segment Source
*/


pop_cat_tapped = 
        LOAD '$GOLD_SEGMENT_EVENTS_GPAP_DB.dq_popular_category_tapped'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
pop_cat_tapped_fltr = FILTER pop_cat_tapped BY edh_bus_date == '$EDH_BUS_DATE';

/* Check if user_id is null. If user_id  is null then populate both member_id and user_id as missing */
pop_cat_tapped_fltr_mark_check_user_id = FOREACH pop_cat_tapped_fltr GENERATE
                                (chararray)id AS id,
                                est_sent_at AS est_sent_at,
                                (user_id IS NULL OR user_id =='' ? $NUMERIC_MISSING_KEY : (int) user_id) AS user_id,
                                (user_id IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS member_id;

/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT pop_cat_tapped_fltr_mark_check_user_id INTO
                    pop_cat_tapped_user_id_missing IF (user_id == $NUMERIC_MISSING_KEY),
                    pop_cat_tapped_user_id_available IF (user_id != $NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_pop_cat_tapped_user_id_available_members = FOREACH (JOIN pop_cat_tapped_user_id_available BY user_id LEFT , dim_members BY user_id )
                         GENERATE   pop_cat_tapped_user_id_available::id AS id,
                                    pop_cat_tapped_user_id_available::est_sent_at AS est_sent_at,
                                    (dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_members::member_id) AS member_id;

/* Combine the 2 relations one with missing user_id and the other with user_id available */
un_pop_cat_tapped_members = UNION jn_pop_cat_tapped_user_id_available_members, pop_cat_tapped_user_id_missing;

tf_segment_pop_cat_tapped = FOREACH un_pop_cat_tapped_members 
     GENERATE    id AS id,
                ToDate(ToString(est_sent_at,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
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
                $EVENT_SOURCE_IOS AS event_source:int,
                '$EVENT_SUB_SOURCE_ANDROID' AS event_sub_source,
                (chararray) NULL AS search_text,
                $TF_POP_CATGRY_TAPPED_QTY AS qty,
                $TF_EVENT_KEY AS event_type_key;

STORE tf_segment_pop_cat_tapped 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();