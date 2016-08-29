/*
PIG SCRIPT    : tf_fact_wm_legacy_profile_view.pig
AUTHOR        : Anil Aleppy
DATE          : 26 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Profile view from Legacy Source
*/


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

        

rc_filtered = FILTER requested_companies BY edh_bus_date == '2016-01-01'
                                AND sp_id IS NOT NULL ;


                                jn_rc_filtered_members = FOREACH (JOIN rc_filtered BY (member_id IS NULL ? -1 : member_id) LEFT , dim_members BY member_id ) 
                         GENERATE   (chararray)rc_filtered::gave_id AS id,
                                    rc_filtered::est_gave_date AS est_gave_date,
                                    rc_filtered::request_info_id AS request_info_id,
                                    rc_filtered::sp_id AS sp_id,
                                    rc_filtered::gave_count AS qty,
                                    (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id,
                                    (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id;

jn_rc_filtered_members_sp = FOREACH (JOIN jn_rc_filtered_members BY (sp_id IS NULL ? -1 : sp_id ) LEFT , dim_service_provider BY legacy_spid)
                         GENERATE   jn_rc_filtered_members::id AS id,
                                    jn_rc_filtered_members::est_gave_date AS est_gave_date,
                                    jn_rc_filtered_members::request_info_id AS request_info_id,
                                    jn_rc_filtered_members::qty AS qty,
                                    jn_rc_filtered_members::member_id AS member_id,
                                    jn_rc_filtered_members::user_id AS user_id,
                                    (dim_service_provider::legacy_spid IS NULL ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
                                    (dim_service_provider::new_world_spid IS NULL ? -3 : dim_service_provider::new_world_spid) AS new_world_spid;
                                    

jn_rc_filtered_members_sp_rti = FOREACH (JOIN jn_rc_filtered_members_sp BY request_info_id , request_type_info BY request_info_id)
                         GENERATE   jn_rc_filtered_members_sp::id AS id,
                                    jn_rc_filtered_members_sp::est_gave_date AS est_gave_date,
                                    jn_rc_filtered_members_sp::qty AS qty,
                                    jn_rc_filtered_members_sp::member_id AS member_id,
                                    jn_rc_filtered_members_sp::user_id AS user_id,
                                    jn_rc_filtered_members_sp::legacy_spid AS legacy_spid,
                                    jn_rc_filtered_members_sp::new_world_spid AS new_world_spid,
                                    request_type_info::request_info_id AS request_info_id,
                                    (request_type_info::cat_id IS NULL ? -1 : request_type_info::cat_id)  AS category_id,
                                    request_type_info::keyword AS search_text;
                                    

tf_legacy_profile_view = FOREACH jn_rc_filtered_members_sp_rti 
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
     
STORE tf_legacy_profile_view 
    INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();