/*
PIG SCRIPT    : tf_fact_wm_07_segement_review_submitted.pig
AUTHOR        : Abhijeet Purwar
DATE          : 29 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Review_submitted from Segment Source
*/


review = 
        LOAD 'gold_sgmnt_events_alwp.dq_review_submitted'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider =
        LOAD 'gold_shared_dim.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/*request_type_info = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_request_type_info'
        USING org.apache.hive.hcatalog.pig.HCatLoader();*/
       
reviw_filtered = FILTER review BY edh_bus_date == '2016-01-01';


/* Check if user_id is null. If user_id  is null then populate both member_id and user_id as missing */
reviw_filtered_mark_check_user_id = FOREACH reviw_filtered GENERATE
                                (chararray)gave_id AS id,
                                est_sent_at AS est_sent_at,
                                request_info_id AS request_info_id,
                                sp_id AS new_world_spid,
                                gave_count AS qty,
                                (user_id IS NULL OR user_id =='' ? -1 : user_id) AS user_id,
                                (user_id IS NULL ? -1 : NULL ) AS member_id;
                   
/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT reviw_filtered_mark_check_user_id INTO
                    review_user_id_missing IF (user_id == -1),
                    review_user_id_available IF (user_id != -1);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_review_user_id_available_members = FOREACH (JOIN review_user_id_available BY user_id LEFT , dim_members BY user_id ) 
                         GENERATE   review_user_id_available::id AS id,
                                    review_user_id_available::est_sent_at AS est_sent_at,
                                    review_user_id_available::request_info_id AS request_info_id,
                                    review_user_id_available::new_world_spid AS new_world_spid,
                                    review_user_id_available::qty AS qty,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;


/* Combine the 2 relations one with missing member_id and the other with member_id available */
all_review_members = UNION review_user_id_missing, jn_review_user_id_available_members;


/* Check if new_world_spid is null. If new_world_spid is null then populate both legacy_spid and new_world_spid as missing */                                    
all_review_members_check_sp_id = FOREACH all_review_members GENERATE
                                id AS id,
                                est_sent_at AS est_sent_at,
                                request_info_id AS request_info_id,
                                qty AS qty,
                                user_id AS user_id,
                                member_id AS member_id,
                                (new_world_spid IS NULL OR new_world_spid =='' ? -1 : new_world_spid ) AS new_world_spid,
                                (new_world_spid IS NULL ? -1 : NULL ) AS legacy_spid;
                            
/* Split into 2 separate relations the records with new_world_spid missing and those with new_world_spid available */
SPLIT all_review_members_check_new_world_spid INTO
                    review_members_new_world_spid_missing IF (new_world_spid == -1),
                    review_members_new_world_spid_available IF (new_world_spid != -1);                        


/* Join with service_provider table to get the corresponding legacy_spid for a given new_world_spid */
jn_review_members_new_world_spid_available = FOREACH (JOIN review_members_new_world_spid_available BY new_world_spid LEFT , dim_service_provider BY new_world_spid ) 
                         GENERATE   review_members_new_world_spid_available::id AS id,
                                    review_members_new_world_spid_available::est_sent_at AS est_sent_at,
                                    review_members_new_world_spid_available::request_info_id AS request_info_id,
                                    review_members_new_world_spid_available::qty AS qty,
                                    review_members_legacy_spid_available::user_id AS user_id,
                                    review_members_new_world_spid_available::member_id  AS member_id,
                                    (dim_service_provider::new_world_spid IS NULL ? -3 : dim_service_provider::new_world_spid) AS new_world_spid,
                                    (dim_service_provider::legacy_spid IS NULL ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
                                    

/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
all_review_members_sp = UNION review_members_new_world_spid_missing, jn_review_members_new_world_spid_available;


                                   
/* Join with request_type_info table to retrieve category_id and keyword */
/*jn_review_members_sp_rti = FOREACH (JOIN all_review_members_sp BY request_info_id , request_type_info BY request_info_id)
                         GENERATE   all_review_members_sp::id AS id,
                                    all_review_members_sp::est_sent_at AS est_sent_at,
                                    all_review_members_sp::qty AS qty,
                                    all_review_members_sp::user_id AS user_id,
                                    all_review_members_sp::member_id AS member_id,
                                    all_review_members_sp::legacy_spid AS legacy_spid,
                                    all_review_members_sp::new_world_spid AS new_world_spid,
                                    (request_type_info::cat_id IS NULL ? -1 : request_type_info::cat_id)  AS category_id,
                                    request_type_info::keyword AS search_text;*/

/* Format the record as per the Target Table structure */
tf_segment_review_submitted = FOREACH all_review_members_sp 
    GENERATE    id AS id, 
                (INT)(ToString(est_sent_at,'yyyyMMdd')) AS date_ak,
                ToString(est_sent_at,'HH:mm') AS time_ak,
                legacy_spid AS legacy_spid,
                new_world_spid AS new_world_spid,
                -2 AS source_ak,
                'Not Applicable' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                -2 AS category_id,
                'Review Submitted' AS event_type,
                'Not Applicable' AS search_type,
                0 AS event_source:int,
                'web' AS event_sub_source,
                NULL AS search_text,
                1 AS qty,
                7 AS event_type_key;

/* Store Data into target table */
STORE tf_segment_review_submitted 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();
