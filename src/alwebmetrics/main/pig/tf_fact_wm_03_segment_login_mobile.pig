/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_03_segment_login_mobile.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_fact_wm_03_segment_login_mobile.
#########################################################################################################*/
/* Load Required Tables */

dq_on_board_user_signs_in_success=
        LOAD '$GOLD_SEGMENT_EVENTS_GPAP_DB.dq_on_board_user_signs_in_success'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

		
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter edh_bus_date to process records only for the passed date */
dq_on_board_user_signs_in_success = FILTER dq_on_board_user_signs_in_success BY edh_bus_date=='$EDH_BUS_DATE' AND user_id != 'no_user_id';

/*Process Started for dq_user_login*/
/* Check if user_id is null as user_id in the applicable column. If user_id is null then populate both member_id and user_id as missing */
sel_obusis =  FOREACH dq_on_board_user_signs_in_success GENERATE
                           (CHARARRAY)id AS (id:CHARARRAY),
                           (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (member_id:INT),
						   (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)user_id) AS (user_id:INT),
						   est_sent_at AS est_sent_at;
						   
/* Split into 2 separate relations the records with user_id missing and those with member_id available */
SPLIT sel_obusis INTO
                    obusis_user_id_missing IF (user_id == (INT)$NUMERIC_MISSING_KEY),
                    obusis_user_id_available IF (user_id != (INT)$NUMERIC_MISSING_KEY);
					
/* Join with dim_member table to get the corresponding membr_id for a given user_id */
jn_obusis_user_id_available_users = FOREACH (JOIN obusis_user_id_available BY user_id LEFT , table_dim_member BY user_id) 
                                   GENERATE 
								            obusis_user_id_available::id AS id,
								            table_dim_member::member_id AS member_id,
								            obusis_user_id_available::user_id AS user_id,
											obusis_user_id_available::est_sent_at AS est_sent_at
											;
											
/* Combine the 2 relations one with missing user_id and the other with member_id available */
un_obusis = UNION jn_obusis_user_id_available_users,obusis_user_id_missing;

/* Format the record as per the Target Table structure */		
tf_login_mobile = FOREACH un_obusis  GENERATE 
             (CHARARRAY)id AS (id:CHARARRAY),
             (INT)(ToString(est_sent_at,'YYYYMMDD')) AS (date_ak:INT),
             ToString(est_sent_at,'hh:mm') AS (time_ak:CHARARRAY),
             (INT)$NUMERIC_NA_KEY AS (legacy_spid:INT),
             (INT)$NUMERIC_NA_KEY AS (new_world_spid:INT),
             (INT)$NUMERIC_NA_KEY AS (source_ak:int),
             (CHARARRAY)'$STRING_NA_VALUE' AS (source_table:chararray),
             (member_id IS NULL?(INT)$NUMERIC_UNKOWN_KEY: member_id) AS (member_id:INT),
             (user_id IS NULL?(INT)$NUMERIC_UNKOWN_KEY:user_id) AS (user_id:INT),
             (INT)$NUMERIC_NA_KEY AS (category_id:INT),
             'Login-Mobile' AS (event_type:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_type:CHARARRAY),
             (INT)$EVENT_SOURCE_ANDROID AS (event_source:INT),
             (CHARARRAY)'$EVENT_SUB_SOURCE_ANDROID' AS (event_sub_source:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_text:CHARARRAY),
            (INT)1 AS (qty:INT),
			(CHARARRAY)'3' AS (event_type_key:CHARARRAY);

/* Store Data into target table */
STORE tf_login_mobile
	INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();
	

