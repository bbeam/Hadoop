/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_15_segment_search_the list.pig
AUTHOR                                  :Varun Rauthan
DATE                                    :Aug 29 2016
DESCRIPTION                             :Data Transformation script for webmetrics fact table for the event
										 'search the list' from Segment Source
#########################################################################################################*/

table_dq_search_the_list=
        LOAD '$GOLD_SEGMENT_EVENTS_GPP_DB.dq_search_the_list'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */
table_dq_search_the_list = FILTER table_dq_search_the_list 
						   BY edh_bus_date=='$EDH_BUS_DATE';

/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
table_dq_search_the_list_check_user_id = FOREACH table_dq_search_the_list
										   GENERATE id AS id,
										   			est_sent_at AS est_sent_at,
										   			(user_id IS NULL OR user_id =='' ? -1 : (INT)user_id) AS user_id,
                                					(user_id IS NULL OR user_id ==''? -1 : NULL ) AS member_id,
										   			event_text AS event_text,
										   			sp_keyword AS sp_keyword;


/* Split into 2 separate relations the records with user_id missing and those with user_id available */
SPLIT table_dq_search_the_list_check_user_id INTO
                    table_dq_search_the_list_user_id_missing IF user_id == -1,
                    table_dq_search_the_list_user_id_available IF user_id != -1;


/* Join with dim_member table to get the corresponding member_id for a given user_id */
gen_table_dq_search_the_list_user_id_available = FOREACH (JOIN table_dq_search_the_list_user_id_available BY (INT)user_id LEFT OUTER, table_dim_member BY user_id)
												 GENERATE id AS id,
												 		  est_sent_at AS est_sent_at,
												 		  (table_dim_member::user_id IS NULL?-3:(INT)table_dim_member::user_id) AS user_id:int,
												 		  (table_dim_member::member_id IS NULL?-3:(INT)table_dim_member::member_id) AS member_id:int,
														  event_text AS event_text,
										   			      sp_keyword AS sp_keyword;
										   			

/* Combine the 2 relations one with missing member_id and the other with member_id available */
join_table_dq_search_the_list_check_user_id = UNION table_dq_search_the_list_user_id_missing, gen_table_dq_search_the_list_user_id_available;



tf_alwp_search_the_list = FOREACH join_table_dq_search_the_list_check_user_id 
						  GENERATE   id AS id,
						  			 (INT)(ToString(est_sent_at,'yyyyMMdd')) AS date_ak:int,
						             ToString(est_sent_at,'HH:mm') AS time_ak:chararray,
						             $NUMERIC_NA_KEY AS legacy_spid:int,
						             $NUMERIC_NA_KEY AS new_world_spid:int,
						             $NUMERIC_NA_KEY AS source_ak:int,
						             '$STRING_NA_VALUE' AS source_table:chararray,
						             member_id AS member_id:int,
						             user_id AS user_id:int,
						             $NUMERIC_NA_KEY AS category_id:int,
						             event_text AS event_type:chararray,
						             '$TF_EVENT_NAME' AS search_type:chararray,
						             $EVENT_SOURCE_IOS AS event_source:int,
						             '$EVENT_SUB_SOURCE_IOS' AS event_sub_source:chararray,
						             sp_keyword AS search_text:chararray,
						             $TF_EVENT_QTY AS qty:int,
                					 $TF_EVENT_KEY AS event_type_key:int;
                                                
                                                
STORE tf_alwp_search_the_list 
	INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();