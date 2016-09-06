/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_16_17_18_legacy_searched_sp.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_fact_wm_17_18_19_legacy_searched_sp.
#########################################################################################################*/
/* Load Required Tables */

dq_searched_sp =
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_searched_sp'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

		
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter edh_bus_date to process records only for the passed date */
dq_searched_sp = FILTER dq_searched_sp BY edh_bus_date=='$EDH_BUS_DATE' AND LOWER(search_params_type) IN ('category','service_provider','keyword');

/*Process Started for dq_user_login*/
/* Check if user_id is null as user_id in the applicable column. If user_id is null then populate both member_id and user_id as missing */
sel_ss =  FOREACH dq_searched_sp GENERATE
                           (CHARARRAY)id AS (id:CHARARRAY),
                           (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (member_id:INT),
						   (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)user_id) AS (user_id:INT),
						   (search_params_filters_categories IS NULL OR search_params_filters_categories == ''? (INT)$NUMERIC_MISSING_KEY : (INT)(SUBSTRING(search_params_filters_categories,1,(INT)SIZE(search_params_filters_categories)-1))) AS (search_params_filters_categories:INT),
						   (search_params_type IS NULL OR search_params_type == ''? '$STRING_MISSING_VALUE' : CONCAT('SP Search - ',LOWER(search_params_type))) AS (search_params_type:CHARARRAY),
						   (search_params_query IS NULL OR search_params_query == ''? '$STRING_MISSING_VALUE' : search_params_query) AS (search_params_query:CHARARRAY),
						   est_sent_at AS est_sent_at;
						   
						   
/* Split into 2 separate relations the records with user_id missing and those with member_id available */
SPLIT sel_ss INTO
                    ss_user_id_missing IF (user_id == (INT)$NUMERIC_MISSING_KEY),
                    ss_user_id_available IF (user_id != (INT)$NUMERIC_MISSING_KEY);
					
/* Join with dim_member table to get the corresponding membr_id for a given user_id */
jn_ss_user_id_available_users = FOREACH (JOIN ss_user_id_available BY user_id LEFT , table_dim_member BY user_id) 
                                   GENERATE 
								            ss_user_id_available::id AS id,
								            table_dim_member::member_id AS member_id,
								            table_dim_member::user_id AS user_id,
											search_params_filters_categories AS search_params_filters_categories,
											search_params_type AS search_params_type,
											search_params_query AS search_params_query,
											ss_user_id_available::est_sent_at AS est_sent_at
											;
											
/* Combine the 2 relations one with missing user_id and the other with member_id available */
un_ss = UNION jn_ss_user_id_available_users,ss_user_id_missing;

/* Format the record as per the Target Table structure */		
tf_searched_sp = FOREACH un_ss  GENERATE 
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
              'Searched SP' AS (event_type:CHARARRAY),
             search_params_type AS (search_type:CHARARRAY),
             (INT)$EVENT_SOURCE_WEB AS (event_source:INT),
             (CHARARRAY)'$EVENT_SUB_SOURCE_WEB' AS (event_sub_source:CHARARRAY),
             search_params_query AS (search_text:CHARARRAY),
            (INT)1 AS (qty:INT),
			(search_params_type=='SP Search - category'?'16':(search_params_type=='SP Search - service_provider'?'17':(search_params_type=='SP Search - keyword'?'18':'$STRING_MISSING_VALUE'))) AS (event_type_key:CHARARRAY);

/* Store Data into target table */
STORE tf_searched_sp
	INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();
	

