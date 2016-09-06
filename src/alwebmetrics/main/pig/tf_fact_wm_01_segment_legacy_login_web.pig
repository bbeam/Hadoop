/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_login.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_angieslistweb_prod_user_login.
#########################################################################################################*/
/* Load Required Tables */
table_dq_member_logon_history = 
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_member_logon_history'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		

table_dq_user_login=
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_user_login'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_user_login_successful =
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_user_login_successful'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

		
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter edh_bus_date to process records only for the passed date */
table_dq_member_logon_history = FILTER table_dq_member_logon_history BY edh_bus_date=='$EDH_BUS_DATE';
table_dq_user_login = FILTER table_dq_user_login BY edh_bus_date=='$EDH_BUS_DATE' AND user_id != 'no_user_id';

/*Process Started for dq_user_login*/
/* Check if user_id is null as user_id in the applicable column. If user_id is null then populate both member_id and user_id as missing */
sel_ul =  FOREACH table_dq_user_login GENERATE
                           (CHARARRAY)id AS (id:CHARARRAY),
                           (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (member_id:INT),
						   (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)user_id) AS (user_id:INT),
						   est_sent_at AS est_sent_at;
						   
/* Split into 2 separate relations the records with user_id missing and those with member_id available */
SPLIT sel_ul INTO
                    ul_user_id_missing IF (user_id == $NUMERIC_MISSING_KEY),
                    ul_user_id_available IF (user_id != $NUMERIC_MISSING_KEY);
					
/* Join with dim_member table to get the corresponding membr_id for a given user_id */
jn_ul_user_id_available_users = FOREACH (JOIN ul_user_id_available BY user_id LEFT , table_dim_member BY user_id) 
                                   GENERATE 
								            ul_user_id_available::id AS id,
								            table_dim_member::member_id AS member_id,
								            table_dim_member::user_id AS user_id,
											ul_user_id_available::est_sent_at AS est_sent_at
											;
											
/* Combine the 2 relations one with missing user_id and the other with member_id available */
un_ul = UNION jn_ul_user_id_available_users,ul_user_id_missing;	

/* Check if member_id is null as member_id in the applicable column. If member_id is null then populate both member_id and user_id as missing */
sel_mlh =  FOREACH table_dq_member_logon_history GENERATE
						   (CHARARRAY)member_logon_history_id AS (id:CHARARRAY),
                           (member_id IS NULL OR (CHARARRAY)member_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)member_id) AS (member_id:INT),
						   (member_id IS NULL OR (CHARARRAY)member_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (user_id:INT),
						    est_logon_date AS est_sent_at;
						   
/* Split into 2 separate relations the records with member_id missing and those with user_id available */

SPLIT sel_mlh INTO
                    ul_member_id_missing IF (member_id == (INT)$NUMERIC_MISSING_KEY),
                    ul_member_id_available IF (member_id != (INT)$NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_mhl_member_id_available_members = FOREACH (JOIN ul_member_id_available BY member_id LEFT , table_dim_member BY member_id) 
                                   GENERATE ul_member_id_available::id AS id,
								            table_dim_member::member_id AS member_id,
								            table_dim_member::user_id AS user_id,
											ul_member_id_available::est_sent_at AS est_sent_at
											;
											
/* Combine the 2 relations one with missing member_id and the other with user_id available */
un_mhl= UNION jn_mhl_member_id_available_members,ul_member_id_missing;


/* Filter edh_bus_date to process records only for the passed date */
dq_user_login_successful = FILTER dq_user_login_successful BY edh_bus_date=='$EDH_BUS_DATE' AND user_id != 'no_user_id';

/*Process Started for dq_user_login*/
/* Check if user_id is null as user_id in the applicable column. If user_id is null then populate both member_id and user_id as missing */
sel_uls =  FOREACH dq_user_login_successful GENERATE
                           (CHARARRAY)id AS (id:CHARARRAY),
                           (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (member_id:INT),
						   (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)user_id) AS (user_id:INT),
						   est_sent_at AS est_sent_at;
						   
/* Split into 2 separate relations the records with user_id missing and those with member_id available */
SPLIT sel_uls INTO
                    uls_user_id_missing IF (user_id == (INT)$NUMERIC_MISSING_KEY),
                    uls_user_id_available IF (user_id != (INT)$NUMERIC_MISSING_KEY);
					
/* Join with dim_member table to get the corresponding membr_id for a given user_id */
jn_uls_user_id_available_users = FOREACH (JOIN uls_user_id_available BY user_id LEFT , table_dim_member BY user_id) 
                                   GENERATE 
								            uls_user_id_available::id AS id,
								            table_dim_member::member_id AS member_id,
								            table_dim_member::user_id AS user_id,
											uls_user_id_available::est_sent_at AS est_sent_at
											;
											
/* Combine the 2 relations one with missing user_id and the other with member_id available */
un_uls = UNION jn_uls_user_id_available_users,uls_user_id_missing;

/* Combine the result set of user_login and member_login_history */
un_mhl_ul= UNION un_mhl,un_ul,un_uls;

/* Format the record as per the Target Table structure */		
tf_user_login_successful = FOREACH un_mhl_ul  GENERATE 
             (CHARARRAY)id AS (id:CHARARRAY),
              ToDate(ToString(est_sent_at,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
              ToString(est_sent_at,'HH:mm') AS (time_ak:chararray),
             (INT)$NUMERIC_NA_KEY AS (legacy_spid:INT),
             (INT)$NUMERIC_NA_KEY AS (new_world_spid:INT),
             (INT)$NUMERIC_NA_KEY AS (source_ak:int),
             (CHARARRAY)'$STRING_NA_VALUE' AS (source_table:chararray),
             (member_id IS NULL?(INT)$NUMERIC_UNKOWN_KEY: member_id) AS (member_id:INT),
             (user_id IS NULL?(INT)$NUMERIC_UNKOWN_KEY:user_id) AS (user_id:INT),
             (INT)$NUMERIC_NA_KEY AS (category_id:INT),
             'Login-Web-Successful' AS (event_type:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_type:CHARARRAY),
             (INT)$EVENT_SOURCE_WEB AS (event_source:INT),
             (CHARARRAY)'$EVENT_SUB_SOURCE_WEB' AS (event_sub_source:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_text:CHARARRAY),
            (INT)1 AS (qty:INT),
			(CHARARRAY)$TF_EVENT_KEY AS (event_type_key:CHARARRAY);

/* Store Data into target table */
STORE tf_user_login_successful
	INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();
	
