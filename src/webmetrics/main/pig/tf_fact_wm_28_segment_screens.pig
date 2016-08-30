/*
PIG SCRIPT    : tf_fact_wm_28_segment_screens.pig
AUTHOR        : Varun Rauthan
DATE          : 29 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Screens from ios Segment Source
*/


screens = 
        LOAD '$GOLD_SEGMENT_EVENTS_GPP_DB.dq_screens'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members = 
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider = 
		LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
		USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */        
screens_filtered = FILTER screens BY edh_bus_date == '$EDH_BUS_DATE'
                                AND NOT (user_id matches '*.[a-zA-Z].*' );

/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
gen_screens_filtered_check_isnull = FOREACH screens_filtered
										   GENERATE id AS id,
										   			est_sent_at AS est_sent_at,
										   			(sp_id IS NULL OR (CHARARRAY)sp_id =='' ? -1 : NULL) AS legacy_spid:INT,
										   			(sp_id IS NULL OR (CHARARRAY)sp_id =='' ? -1 : (INT)sp_id) AS new_world_spid:INT,
										   			(user_id IS NULL OR user_id =='' ? -1 : (INT)user_id) AS user_id,
                                					(member_id IS NULL OR member_id ==''? -1 : (INT)member_id ) AS member_id,
										   			keyword AS keyword;

/* Split into 4 separate relations the records with all the member_id and user_id combination */
SPLIT gen_screens_filtered_check_isnull INTO
                    table_dq_screens_user_id_missing_member_id_missing IF user_id == -1 and member_id == -1,
                    table_dq_screens_user_id_available_member_id_available IF user_id != -1 and member_id != -1,
					table_dq_screens_user_id_missing_member_id_available IF user_id == -1 and member_id != -1,
					table_dq_screens_user_id_available_member_id_missing IF user_id != -1 and member_id == -1;


gen_table_dq_screens_user_id_missing_member_id_available = FOREACH (JOIN table_dq_screens_user_id_missing_member_id_available BY (INT)member_id LEFT OUTER, dim_members BY member_id ) 
                            GENERATE table_dq_screens_user_id_missing_member_id_available::id AS id,  
                                     table_dq_screens_user_id_missing_member_id_available::est_sent_at AS est_sent_at,
									 table_dq_screens_user_id_missing_member_id_available::legacy_spid AS legacy_spid,
									 table_dq_screens_user_id_missing_member_id_available::new_world_spid AS new_world_spid,
                                     (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                     (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id,
									 table_dq_screens_user_id_missing_member_id_available::keyword AS keyword;
									 
gen_table_dq_screens_user_id_available_member_id_missing = FOREACH (JOIN table_dq_screens_user_id_available_member_id_missing BY (INT)user_id LEFT OUTER, dim_members BY user_id )
							GENERATE table_dq_screens_user_id_available_member_id_missing::id AS id,  
                                     table_dq_screens_user_id_available_member_id_missing::est_sent_at AS est_sent_at,
									 table_dq_screens_user_id_available_member_id_missing::legacy_spid AS legacy_spid,
									 table_dq_screens_user_id_available_member_id_missing::new_world_spid AS new_world_spid,
                                     (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                     (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id,
									 table_dq_screens_user_id_available_member_id_missing::keyword AS keyword;

union_gen_table_dq_screens_user_id_member_id = union gen_table_dq_screens_user_id_missing_member_id_available ,
													 gen_table_dq_screens_user_id_available_member_id_missing,
													 table_dq_screens_user_id_missing_member_id_missing,
													 table_dq_screens_user_id_available_member_id_available;
									 
/* Split into 2 seperate relations the records with sp_id missing and sp_id available */
SPLIT union_gen_table_dq_screens_user_id_member_id INTO
					table_dim_service_provider_sp_id_missing IF new_world_spid == -1,
					table_dim_service_provider_sp_id_available IF new_world_spid != -1;
					
gen_table_dim_service_provider_sp_id_available = FOREACH (JOIN table_dim_service_provider_sp_id_available BY new_world_spid LEFT OUTER, dim_service_provider BY new_world_spid )
												 GENERATE table_dim_service_provider_sp_id_available::id AS id,
												          table_dim_service_provider_sp_id_available::est_sent_at AS est_sent_at,
														  (dim_service_provider::legacy_spid is null ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
														  (dim_service_provider::new_world_spid is null ? -3 : dim_service_provider::new_world_spid) AS new_world_spid,
														  table_dim_service_provider_sp_id_available::user_id AS user_id,
														  table_dim_service_provider_sp_id_available::member_id AS member_id,
														  table_dim_service_provider_sp_id_available::keyword AS keyword;
														  
union_gen_table_screens_user_id_member_id_sp_id = UNION table_dim_service_provider_sp_id_missing, gen_table_dim_service_provider_sp_id_available;
   

tf_segment_screens = FOREACH union_gen_table_screens_user_id_member_id_sp_id 
					 GENERATE   id AS id, 
								(INT)(ToString(est_sent_at,'YYYYMMDD')) AS date_ak,
								ToString(est_sent_at,'hh:mm') AS time_ak,
								legacy_spid AS legacy_spid,
								new_world_spid AS new_world_spid,
								$NUMERIC_NA_KEY AS source_ak,
								'$STRING_NA_VALUE' AS source_table,
								member_id AS member_id,
								user_id  AS user_id,
								$NUMERIC_NA_KEY AS category_id,
								'$TF_EVENT_NAME' AS event_type,
								'$STRING_NA_VALUE' AS search_type,
								$EVENT_SOURCE_IOS AS event_source:int,
								'$EVENT_SUB_SOURCE_IOS' AS event_sub_source,
								keyword AS search_text,
								$TF_EVENT_QTY AS qty,
								$TF_EVENT_KEY AS event_type_key;
     
STORE tf_segment_screens 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();