/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_alwp_offer_page_loaded.pig
AUTHOR                                  :Gaurav Maheshwari
DATE                                    :Thursday Aug 25 2016
DESCRIPTION                             :Pig script to create load tf_fact_webmetrics_angieslistweb_prod_offer_page_loaded.
#########################################################################################################*/

table_dq_offer_page_loaded=
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_offer_page_loaded'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

		
table_dim_event_type=
        LOAD '$GOLD_SHARED_DIM_DB.dim_event_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
dim_members=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider=
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_offer_page_loaded= FILTER table_dq_offer_page_loaded BY edh_bus_date=='$EDHBUSDATE';

/* Check if user_id is null. If user_id id is null then populate both member_id and user_id as missing */
	
offer_page_loaded_rec_check = FOREACH table_dq_offer_page_loaded GENERATE
                                (chararray)id AS id,
                                est_sent_at AS est_sent_at,
                                service_provider_id AS new_world_spid,
                                (user_id IS NULL OR user_id =='' ? $NUMERIC_MISSING_KEY : (INT)user_id) AS user_id,
                                (user_id IS NULL OR user_id ==''? $NUMERIC_MISSING_KEY : NULL ) AS member_id,
	

/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT offer_page_loaded_rec_check INTO
                    rc_user_id_missing   IF  (user_id == $NUMERIC_MISSING_KEY),
                    rc_user_id_available IF (user_id != $NUMERIC_MISSING_KEY);



/* Join with dim_member table to get the corresponding member_id for a given user_id */
jn_rc_user_id_available_members = FOREACH (JOIN rc_user_id_available BY user_id LEFT , dim_members BY user_id ) 
                         GENERATE   rc_user_id_available::id AS id,
                                    rc_user_id_available::est_sent_at AS est_sent_at,
                                    rc_user_id_available::new_world_spid AS new_world_spid,
                                    (dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_members::member_id) AS member_id,
                                    (dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_members::user_id) AS user_id;

/* Combine the 2 relations one with missing member_id and the other with member_id available */
un_rc_members = UNION rc_user_id_missing, jn_rc_user_id_available_members;


/* Check if new_world_spid is null. If service_provider_id is null then populate both legacy_spid and new_world_spid as missing */
un_rc_members_check_new_world_spid = FOREACH un_rc_members GENERATE
                                id AS id,
                                est_sent_at AS est_sent_at,
                                member_id AS member_id,
                                user_id AS user_id,
                                (service_provider_id IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS legacy_spid,
                                (service_provider_id IS NULL ? $NUMERIC_MISSING_KEY : service_provider_id) AS new_world_spid;


/* Split into 2 separate relations the records with new_world_spid missing and those with new_world_spid available */

SPLIT un_rc_members_check_new_world_spid INTO
                    rc_members_new_world_spid_missing IF (new_world_spid == $NUMERIC_MISSING_KEY),
                    rc_members_new_world_spid_available IF (new_world_spid != $NUMERIC_MISSING_KEY);
                                
/* Join with service_provider table to get the corresponding legacy_spid  for a given new_world_spid */

jn_rc_members_new_world_spid_available = FOREACH (JOIN rc_members_new_world_spid_available BY new_world_spid LEFT , dim_service_provider BY new_world_spid ) 
                         GENERATE   rc_members_new_world_spid_available::id AS id,
                                    rc_members_new_world_spid_available::est_gave_date AS est_gave_date,
                                    rc_members_new_world_spid_available::request_info_id AS request_info_id,
                                    rc_members_new_world_spid_available::qty AS qty,
                                    rc_members_new_world_spid_available::member_id  AS member_id,
                                    rc_members_new_world_spid_available::user_id AS user_id,
                                    (dim_service_provider::legacy_spid IS NULL ? $NUMERIC_UNKOWN_KEY : dim_service_provider::legacy_spid) AS legacy_spid,
                                    (dim_service_provider::new_world_spid IS NULL ? $NUMERIC_UNKOWN_KEY : dim_service_provider::new_world_spid) AS new_world_spid;

/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
un_rc_members_sp = UNION rc_members_new_world_spid_missing, jn_rc_members_new_world_spid_available;


tf_offer_page_loaded = FOREACH un_rc_members_sp 
    GENERATE    id AS id:int, 
                (INT)(ToString(est_sent_at,'yyyyMMdd')) AS date_ak,
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
                 $EVENT_SOURCE_WEB AS event_source,
                '$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
                search_text AS search_text,
                1 AS qty,
                $TF_EVENT_KEY AS event_type_key;


	 
STORE final_webmetrics_offer_page_loaded 
	  INTO 'work_al_webmetrics.tf_fact_web_metrics'
	  USING org.apache.hive.hcatalog.pig.HCatStorer();