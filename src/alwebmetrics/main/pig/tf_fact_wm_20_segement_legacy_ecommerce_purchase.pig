/*
PIG SCRIPT    : tf_fact_wm_20_segement_legacy_ecommerce_purchase.pig
AUTHOR        : Abhijeet Purwar
DATE          : 30 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event legacy_ecommerce_purchase from Segment Source
*/

legacy_ecommerce_purchase = 
        LOAD '$GOLD_LEGACY_REPORTS_DBO_DB.dq_store_page_loaded'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
dim_members =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider =
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/*request_type_info = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_request_type_info'
        USING org.apache.hive.hcatalog.pig.HCatLoader();*/
       
store_page_loaded_filtered = FILTER store_page_loaded BY edh_bus_date == '$EDH_BUS_DATE';


/* Check if user_id is null. If user_id  is null then populate both member_id and user_id as missing */
store_page_loaded_filtered_mark_check_user_id = FOREACH store_page_loaded_filtered GENERATE
                                (chararray)id AS id,
                                est_sent_at AS est_sent_at,
                                service_provider_id AS new_world_spid,
                                $TF_STORE_PAGE_LOADED_QTY AS qty,
                                (user_id IS NULL OR user_id =='' ? $NUMERIC_MISSING_KEY : (int) user_id) AS user_id,
                                (user_id IS NULL OR user_id =='' ? $NUMERIC_MISSING_KEY : NULL ) AS member_id;

/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT store_page_loaded_filtered_mark_check_user_id INTO
                    store_page_loaded_user_id_missing IF (user_id == $NUMERIC_MISSING_KEY),
                    store_page_loaded_user_id_available IF (user_id != $NUMERIC_MISSING_KEY);
                    
/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_store_page_loaded_user_id_available_members = FOREACH (JOIN store_page_loaded_user_id_available BY user_id LEFT , dim_members BY user_id ) 
                         GENERATE   store_page_loaded_user_id_available::id AS id,
                                    store_page_loaded_user_id_available::est_sent_at AS est_sent_at,
                                    store_page_loaded_user_id_available::new_world_spid AS new_world_spid,
                                    store_page_loaded_user_id_available::qty AS qty,
                                    (dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_members::member_id) AS member_id;
                                    
                                    
/* Combine the 2 relations one with missing member_id and the other with member_id available */
all_store_page_loaded_members = UNION store_page_loaded_user_id_missing, jn_store_page_loaded_user_id_available_members;

/* Check if new_world_spid is null. If new_world_spid is null then populate both legacy_spid and new_world_spid as missing */                                    
all_store_page_loaded_members_check_new_world_spid = FOREACH all_store_page_loaded_members GENERATE
                                id AS id,
                                est_sent_at AS est_sent_at,
                                qty AS qty,
                                user_id AS user_id,
                                member_id AS member_id,
                                (new_world_spid IS NULL OR new_world_spid == '' ? $NUMERIC_MISSING_KEY : (int) new_world_spid ) AS new_world_spid,
                                (new_world_spid IS NULL OR new_world_spid == '' ? $NUMERIC_MISSING_KEY : NULL ) AS legacy_spid;
                               
/* Split into 2 separate relations the records with new_world_spid missing and those with new_world_spid available */
SPLIT all_store_page_loaded_members_check_new_world_spid INTO
                    store_page_loaded_members_new_world_spid_missing IF (new_world_spid == $NUMERIC_MISSING_KEY),
                    store_page_loaded_members_new_world_spid_available IF (new_world_spid != $NUMERIC_MISSING_KEY); 
                                
/* Join with service_provider table to get the corresponding legacy_spid for a given new_world_spid */
jn_all_store_page_loaded_members_new_world_spid_available = FOREACH (JOIN store_page_loaded_members_new_world_spid_available BY new_world_spid LEFT , dim_service_provider BY new_world_spid ) 
                         GENERATE   store_page_loaded_members_new_world_spid_available::id AS id,
                                    store_page_loaded_members_new_world_spid_available::est_sent_at AS est_sent_at,
                                    store_page_loaded_members_new_world_spid_available::qty AS qty,
                                    store_page_loaded_members_new_world_spid_available::user_id AS user_id,
                                    store_page_loaded_members_new_world_spid_available::member_id  AS member_id,
                                    (dim_service_provider::new_world_spid IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_service_provider::new_world_spid) AS new_world_spid,
                                    (dim_service_provider::legacy_spid IS NULL ? $NUMERIC_UNKOWN_KEY : (int) dim_service_provider::legacy_spid) AS legacy_spid;
  
/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
all_store_page_loaded_members_sp = UNION store_page_loaded_members_new_world_spid_missing, jn_all_store_page_loaded_members_new_world_spid_available;

/* Join with request_type_info table to retrieve category_id and keyword */
/*jn_review_members_sp_rti = FOREACH (JOIN all_review_members_sp BY request_info_id , request_type_info BY request_info_id)
                         GENERATE   all_review_members_sp::id AS id,
                                    all_review_members_sp::est_sent_at AS est_sent_at,
                                    all_review_members_sp::qty AS qty,
                                    all_review_members_sp::user_id AS user_id,
                                    all_review_members_sp::member_id AS member_id,
                                    all_review_members_sp::legacy_spid AS legacy_spid,
                                    all_review_members_sp::new_world_spid AS new_world_spid,
                                    (request_type_info::cat_id IS NULL ? $NUMERIC_MISSING_KEY : request_type_info::cat_id)  AS category_id,
                                    request_type_info::keyword AS search_text;*/

/* Format the record as per the Target Table structure */
tf_segment_store_page_loaded = FOREACH all_store_page_loaded_members_sp 
    GENERATE    id AS id, 
                ToDate(ToString(est_sent_at,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
                ToString(est_sent_at,'HH:mm') AS time_ak,
                legacy_spid AS legacy_spid,
                new_world_spid AS new_world_spid,
                $NUMERIC_NA_KEY AS source_ak,
                '$STRING_NA_VALUE' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                $NUMERIC_NA_KEY AS category_id,
                '$TF_EVENT_NAME' AS event_type,
                '$STRING_NA_VALUE' AS search_type,
                $EVENT_SOURCE_WEB AS event_source:int,
                '$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
                (chararray) NULL AS search_text,
                $TF_STORE_PAGE_LOADED_QTY AS qty,
                $TF_EVENT_KEY AS event_type_key;

/* Store Data into target table */
STORE tf_segment_store_page_loaded 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics1'
    USING org.apache.hive.hcatalog.pig.HCatStorer();
