/*
PIG SCRIPT    : tf_fact_wm_legacy_profile_view.pig
AUTHOR        : Anil Aleppy
DATE          : 26 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Profile view from Legacy Source
*/

/* Load Required Tables */
requested_companies = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_requested_companies'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
request_type_info = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_request_type_info'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
dim_members =
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
dim_service_provider =
        LOAD 'gold_shared_dim.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */
rc_sel = FILTER requested_companies BY edh_bus_date == '2016-01-01'
                                AND sp_id IS NOT NULL ;

/* Check if member_id is null. If member id is null then populate both member_id and user_id as missing */
rc_sel_mark_check_member_id = FOREACH rc_sel GENERATE
                                (chararray)gave_id AS id,
                                est_gave_date AS est_gave_date,
                                request_info_id AS request_info_id,
                                sp_id AS legacy_spid,
                                gave_count AS qty,
                                (member_id IS NULL ? -1 : member_id) AS member_id,
                                (member_id IS NULL ? -1 : NULL ) AS user_id;

/* Split into 2 separate relations the records with member_id missing and those with member_id available */
SPLIT rc_sel_mark_check_member_id INTO
                    rc_member_id_missing IF (member_id == -1),
                    rc_member_id_available IF (member_id != -1);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_rc_member_id_available_members = FOREACH (JOIN rc_member_id_available BY member_id LEFT , dim_members BY member_id ) 
                         GENERATE   rc_member_id_available::id AS id,
                                    rc_member_id_available::est_gave_date AS est_gave_date,
                                    rc_member_id_available::request_info_id AS request_info_id,
                                    rc_member_id_available::legacy_spid AS legacy_spid,
                                    rc_member_id_available::qty AS qty,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id;

/* Combine the 2 relations one with missing member_id and the other with member_id available */
un_rc_members = UNION rc_member_id_missing, jn_rc_member_id_available_members;

/* Check if legacy_spid is null. If sp_id is null then populate both legacy_spid and new_world_spid as missing */
                                    
un_rc_members_check_sp_id = FOREACH un_rc_members GENERATE
                                id AS id,
                                est_gave_date AS est_gave_date,
                                request_info_id AS request_info_id,
                                qty AS qty,
                                member_id AS member_id,
                                user_id AS user_id,
                                (legacy_spid IS NULL ? -1 : legacy_spid ) AS legacy_spid,
                                (legacy_spid IS NULL ? -1 : NULL ) AS new_world_spid;
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