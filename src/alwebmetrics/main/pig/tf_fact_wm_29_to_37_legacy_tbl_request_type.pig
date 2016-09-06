/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_29_to_37_legacy_tbl_request_type.pig
AUTHOR                                  :Abhijeet Purwar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_fact_wm_29_to_37_legacy_tbl_request_type
#########################################################################################################*/
/* Load Required Tables */

dq_tbl_requests =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_tbl_requests'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_tbl_request_type_info =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_tbl_request_type_info'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_tbl_request_type =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_tbl_request_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_member =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider =
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Filter edh_bus_date to process records only for the passed date */
dq_tbl_requests_fltr = FILTER dq_tbl_requests BY edh_bus_date=='$EDH_BUS_DATE';

/* Filter request_type_id to process only valid events */
dq_tbl_request_type_info_fltr = FILTER dq_tbl_request_type_info BY request_type_id IN (1, 2, 3, 4, 5, 7, 8, 9, 10);

/* Join with request_type_info table to retrieve category_id and keyword */
jn_requests_rti = FOREACH (JOIN dq_tbl_requests_fltr BY request_id , dq_tbl_request_type_info_fltr BY request_id)
                         GENERATE   dq_tbl_requests_fltr::request_id AS id,
                                    dq_tbl_requests_fltr::est_request_date AS est_request_date,
                                    dq_tbl_requests_fltr::member_id AS member_id,
                                    dq_tbl_request_type_info_fltr::request_type_id AS request_type_id,
                                    dq_tbl_request_type_info_fltr::sp_id AS legacy_spid,
                                    dq_tbl_request_type_info_fltr::cat_id AS cat_id,
                                    dq_tbl_request_type_info_fltr::keyword AS keyword,
                                    dq_tbl_request_type_info_fltr::request_count AS request_count;

requests_rti_fltr = FILTER jn_requests_rti BY request_type_id != 6;

jn_requests_rti_fltr = FOREACH (JOIN requests_rti_fltr BY request_type_id, dq_tbl_request_type BY request_type_id)
                        GENERATE    requests_rti_fltr::id AS id,
                                    requests_rti_fltr::est_request_date AS est_request_date,
                                    requests_rti_fltr::member_id AS member_id,
                                    requests_rti_fltr::request_type_id AS request_type_id,
                                    requests_rti_fltr::legacy_spid AS legacy_spid,
                                    requests_rti_fltr::cat_id AS cat_id,
                                    requests_rti_fltr::keyword AS keyword,
                                    requests_rti_fltr::request_count AS request_count;

/* Check if member_id is null. If member id is null then populate both member_id and user_id as missing */
requests_check_member_id = FOREACH jn_requests_rti_fltr GENERATE
                                id AS id,
                                est_request_date AS est_request_date,
                                request_type_id AS request_type_id,
                                legacy_spid AS legacy_spid,
                                cat_id AS cat_id,
                                keyword AS keyword,
                                request_count AS request_count,
                                (member_id IS NULL ? $NUMERIC_MISSING_KEY : (int) member_id) AS member_id,
                                (member_id IS NULL ? $NUMERIC_MISSING_KEY :  NULL ) AS user_id;

/* Split into 2 separate relations the records with member_id missing and those with member_id available */
SPLIT requests_check_member_id INTO
                    requests_member_id_missing IF (member_id == $NUMERIC_MISSING_KEY),
                    requests_member_id_available IF (member_id != $NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_requests_member_id_available_members = FOREACH (JOIN requests_member_id_available BY member_id LEFT , dim_member BY member_id ) 
                         GENERATE   requests_member_id_available::id AS id,
                                    requests_member_id_available::est_request_date AS est_request_date,
                                    requests_member_id_available::request_type_id AS request_type_id,
                                    requests_member_id_available::legacy_spid AS legacy_spid,
                                    requests_member_id_available::cat_id AS cat_id,
                                    requests_member_id_available::keyword AS keyword,
                                    requests_member_id_available::request_count AS request_count,
                                    (dim_member::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_member::member_id) AS member_id,
                                    (dim_member::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_member::user_id) AS user_id;

/* Combine the 2 relations one with missing member_id and the other with member_id available */
un_requests_members = UNION requests_member_id_missing, jn_requests_member_id_available_members;

/* Check if legacy_spid is null. If sp_id is null then populate both legacy_spid and new_world_spid as missing */
un_requests_members_check_sp_id = FOREACH un_requests_members GENERATE
                                id AS id,
                                est_request_date AS est_request_date,
                                request_type_id AS request_type_id,
                                cat_id AS cat_id,
                                keyword AS keyword,
                                request_count AS request_count,
                                member_id AS member_id,
                                user_id AS user_id,
                                (legacy_spid IS NULL ? $NUMERIC_MISSING_KEY : legacy_spid ) AS legacy_spid,
                                (legacy_spid IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS new_world_spid;

/* Split into 2 separate relations the records with legacy_spid missing and those with legacy_spid available */
SPLIT un_requests_members_check_sp_id INTO
                    requests_members_legacy_spid_missing IF (legacy_spid == $NUMERIC_MISSING_KEY),
                    requests_members_legacy_spid_available IF (legacy_spid != $NUMERIC_MISSING_KEY);


/* Join with service_provider table to get the corresponding new_world_spid for a given legacy_spid */
jn_requests_members_legacy_spid_available = FOREACH (JOIN requests_members_legacy_spid_available BY legacy_spid LEFT , dim_service_provider BY legacy_spid ) 
                         GENERATE   requests_members_legacy_spid_available::id AS id,
                                    requests_members_legacy_spid_available::est_request_date AS est_request_date,
                                    requests_members_legacy_spid_available::request_type_id AS request_type_id,
                                    requests_members_legacy_spid_available::cat_id AS cat_id,
                                    requests_members_legacy_spid_available::keyword  AS keyword,
                                    requests_members_legacy_spid_available::request_count AS request_count,
                                    requests_members_legacy_spid_available::member_id  AS member_id,
                                    requests_members_legacy_spid_available::user_id AS user_id,
                                    (dim_service_provider::legacy_spid IS NULL ? $NUMERIC_UNKOWN_KEY : dim_service_provider::legacy_spid) AS legacy_spid,
                                    (dim_service_provider::new_world_spid IS NULL ? $NUMERIC_UNKOWN_KEY : dim_service_provider::new_world_spid) AS new_world_spid;

/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
un_requests_members_sp = UNION requests_members_legacy_spid_missing, jn_requests_members_legacy_spid_available;


/* Format the record as per the Target Table structure */
tf_legacy_tbl_request_type = FOREACH un_requests_members_sp 
    GENERATE    id AS id,
                ToDate(ToString(est_request_date,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
                ToString(est_request_date,'HH:mm') AS time_ak,
                legacy_spid AS legacy_spid,
                new_world_spid AS new_world_spid,
                $NUMERIC_NA_KEY AS source_ak,
                '$STRING_NA_VALUE' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                cat_id AS category_id,
                (request_type_id == 1 ? 'Category Request' :(request_type_id == 2 ? 'Service Provider Request':(request_type_id == 3 ? 'Keyword Request':(request_type_id == 4 ? 'MyAngie Request':(request_type_id == 5 ? 'Coupon Request':(request_type_id == 7 ? 'KeywordBought Request':(request_type_id == 8 ? 'Blended Request':(request_type_id == 9 ? 'Quick Quote Request':(request_type_id == 10 ? 'BigDeal Feedback Interstitial Page':'$STRING_MISSING_VALUE'))))))))) as event_type,
                '$STRING_NA_VALUE' AS search_type,
                $EVENT_SOURCE_WEB AS event_source:int,
                '$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
                keyword AS search_text,
                request_count AS qty,
                $TF_EVENT_KEY AS event_type_key;


/* Store Data into target table */
STORE tf_legacy_tbl_request_type
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();
