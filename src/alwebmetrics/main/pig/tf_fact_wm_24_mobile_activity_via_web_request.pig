/*
PIG SCRIPT    : tf_fact_wm_25_mobile_activity_via_web_request.pig
AUTHOR        : Anil Aleppy
DATE          : 4 Sep 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Mobile Activity via web request
*/

/* Load Required Tables */
web_request = 
        LOAD '$GOLD_LEGACY_WEBLOGGING_DBO_DB.dq_web_request'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
dim_members =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter by edh_bus_date to process records for previous day only. */
sel_web_request = FILTER web_request BY edh_bus_date == '$EDH_BUS_DATE' ; 

/* Check if member_id is null. If member id is null then populate both member_id and user_id as missing */
sel_web_request_check_member_id = FOREACH sel_web_request GENERATE
                                (chararray)request_id AS id,
                                est_time_utc AS est_time_utc,
                                (member_id IS NULL ? $NUMERIC_MISSING_KEY : member_id) AS member_id,
                                (member_id IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS user_id;

/* Split into 2 separate relations the records with member_id missing and those with member_id available */
SPLIT sel_web_request_check_member_id INTO
                    web_request_member_id_missing IF (member_id == $NUMERIC_MISSING_KEY),
                    web_request_member_id_available IF (member_id != $NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_web_request_member_id_available_members = FOREACH (JOIN web_request_member_id_available BY member_id LEFT , dim_members BY member_id ) 
                         GENERATE   web_request_member_id_available::id AS id,
                                    web_request_member_id_available::est_time_utc AS est_time_utc,
                                    (dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_members::member_id) AS member_id,
                                    (dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_members::user_id) AS user_id;

/* Combine the 2 relations one with missing member_id and the other with member_id available */
un_eb_request_members = UNION web_request_member_id_missing, jn_web_request_member_id_available_members;

/* Format the record as per the Target Table structure */
tf_legacy_macwr = FOREACH un_eb_request_members 
    GENERATE    id AS id, 
                ToDate(ToString(est_time_utc,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak),
                ToString(est_time_utc,'HH:mm') AS time_ak,
                $NUMERIC_NA_KEY AS legacy_spid,
                $NUMERIC_NA_KEY AS new_world_spid,
                $NUMERIC_NA_KEY AS source_ak,
                '$STRING_NA_VALUE' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                $NUMERIC_NA_KEY AS category_id,
                '$TF_EVENT_NAME' AS event_type,
                '$STRING_NA_VALUE' AS search_type,
                $EVENT_SOURCE_WEB AS event_source,
                '$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
                (chararray) NULL AS search_text,
                1 AS qty,
                $TF_EVENT_KEY AS event_type_key;

/* Store Data into target table */
STORE tf_legacy_macwr 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();