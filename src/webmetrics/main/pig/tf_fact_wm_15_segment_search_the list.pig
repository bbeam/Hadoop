/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_16_segment_search_the list.pig
AUTHOR                                  :Varun Rauthan
DATE                                    :Aug 29 2016
DESCRIPTION                             :Data Transformation script for webmetrics fact table for the event
										 'search the list' from Segment Source
#########################################################################################################*/

table_dq_search_the_list=
        LOAD 'gold_sgmnt_events_gpp.dq_search_the_list'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dim_member=
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */
table_dq_search_the_list = FILTER table_dq_search_the_list 
						   BY edh_bus_date=='$EDH_BUS_DATE' 
						   AND NOT (user_id matches '*.[a-zA-Z].*' );

/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
table_dq_search_the_list_check_user_id = FOREACH table_dq_search_the_list
										   GENERATE est_sent_at AS est_sent_at,
										   			(user_id IS NULL ? -1 : (INT)user_id) AS user_id,
                                					(user_id IS NULL ? -1 : NULL ) AS member_id,
										   			event_text AS event_text,
										   			sp_keyword AS sp_keyword;


/* Split into 2 separate relations the records with member_id missing and those with member_id available */
SPLIT table_dq_search_the_list INTO
                    table_dq_search_the_list_user_id_missing IF ((INT)user_id == -1),
                    table_dq_search_the_list_user_id_available IF ((INT)user_id != -1);

		
tf_alwp_search_the_list = FOREACH (JOIN table_dq_search_the_list_user_id_available BY (INT)user_id LEFT OUTER, table_dim_member BY user_id) 
						  GENERATE   id AS id,
						  			 (INT)(ToString(table_dq_search_the_list_user_id_available::est_sent_at,'yyyyMMdd')) AS date_ak:int,
						             ToString(table_dq_search_the_list_user_id_available::est_sent_at,'HH:mm') AS time_ak:chararray,
						             -2 AS legacy_spid:int,
						             -2 AS new_world_spid:int,
						             -2 AS source_ak:int,
						             'Not Applicable' AS source_table:chararray,
						             (table_dim_member::member_id IS NULL?-3:(INT)table_dim_member::member_id) AS member_id:int,
						             (table_dim_member::user_id IS NULL?-3:(INT)table_dim_member::user_id) AS user_id:int,
						             -2 AS category_id:int,
						             table_dq_search_the_list_user_id_available::event_text AS event_type:chararray,
						             'Search The List' AS search_type:chararray,
						             1 AS event_source:int,
						             'ios' AS event_sub_source:chararray,
						             table_dq_search_the_list::sp_keyword AS search_text:chararray,
						             1 AS qty:int,
                					 15 AS event_type_key:int;
                                                
                                                
STORE tf_alwp_search_the_list 
	INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();