/*
PIG SCRIPT    : tf_fact_wm_21_segement_store_page_loaded.pig
AUTHOR        : Abhijeet Purwar
DATE          : 30 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event store_page_loaded from Segment Source
*/

store_page_loaded = 
        LOAD 'gold_sgmnt_events_alwp.dq_store_page_loaded'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
stl_search_fltr = FILTER stl_search BY edh_bus_date == '2016-01-01';


dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider =
        LOAD 'gold_shared_dim.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

request_type_info = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_request_type_info'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
       
reviw_filtered = FILTER review BY edh_bus_date == '2016-01-01';


/* Check if user_id is null. If user_id  is null then populate both member_id and user_id as missing */
reviw_filtered_mark_check_member_id = FOREACH reviw_filtered GENERATE
                                (chararray)gave_id AS id,
                                est_sent_at AS est_sent_at,
                                request_info_id AS request_info_id,
                                sp_id AS new_world_spid,
                                gave_count AS qty,
                                (user_id IS NULL ? -1 : user_id) AS user_id,
                                (user_id IS NULL ? -1 : NULL ) AS member_id;

/* Split into 2 separate relations the records with legacy_spid missing and those with legacy_spid available */

SPLIT un_rc_members_check_sp_id INTO
                    rc_members_legacy_spid_missing IF (legacy_spid == -1),
                    rc_members_legacy_spid_available IF (legacy_spid != -1);
                                
/* Join with service_provider table to get the corresponding new_world_spid for a given legacy_spid */

jn_rc_members_legacy_sp_id_available = FOREACH (JOIN rc_members_legacy_spid_available BY legacy_spid LEFT , dim_service_provider BY legacy_spid ) 
                         GENERATE   rc_members_legacy_spid_available::id AS id,
                                    rc_members_legacy_spid_available::est_gave_date AS est_gave_date,
                                    rc_members_legacy_spid_available::request_info_id AS request_info_id,
                                    rc_members_legacy_spid_available::qty AS qty,
                                    rc_members_legacy_spid_available::member_id  AS member_id,
                                    rc_members_legacy_spid_available::user_id AS user_id,
                                    (dim_service_provider::legacy_spid IS NULL ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
                                    (dim_service_provider::new_world_spid IS NULL ? -3 : dim_service_provider::new_world_spid) AS new_world_spid;

/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
un_rc_members_sp = UNION rc_members_legacy_sp_id_missing, jn_rc_members_legacy_sp_id_available;
                                    
/* Join with request_type_info table to retrieve category_id and keyword */
jn_rc_members_sp_rti = FOREACH (JOIN un_rc_members_sp BY request_info_id , request_type_info BY request_info_id)
                         GENERATE   un_rc_members_sp::id AS id,
                                    un_rc_members_sp::est_gave_date AS est_gave_date,
                                    un_rc_members_sp::qty AS qty,
                                    un_rc_members_sp::member_id AS member_id,
                                    un_rc_members_sp::user_id AS user_id,
                                    un_rc_members_sp::legacy_spid AS legacy_spid,
                                    un_rc_members_sp::new_world_spid AS new_world_spid,
                                    (request_type_info::cat_id IS NULL ? -1 : request_type_info::cat_id)  AS category_id,
                                    request_type_info::keyword AS search_text;
                                    

/* Format the record as per the Target Table structure */
tf_legacy_profile_view = FOREACH jn_rc_members_sp_rti 
    GENERATE    id AS id, 
                (INT)(ToString(est_gave_date,'yyyyMMdd')) AS date_ak,
                ToString(est_gave_date,'HH:mm') AS time_ak,
                legacy_spid AS legacy_spid,
                new_world_spid AS new_world_spid,
                -2 AS source_ak,
                'Not Applicable' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                category_id AS category_id,
                'Profile View' AS event_type,
                'Not Applicable' AS search_type,
                0 AS event_source:int,
                'web' AS event_sub_source,
                search_text AS search_text,
                qty AS qty,
                7 AS event_type_key;

/* Store Data into target table */
STORE tf_legacy_profile_view 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();