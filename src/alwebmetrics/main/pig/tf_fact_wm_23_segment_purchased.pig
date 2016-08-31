/*
PIG SCRIPT    : tf_fact_wm_23_segment_purchased.pig
AUTHOR        : Varun Rauthan
DATE          : 31 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event purchased from web Segment Source
*/


dq_purchased = 
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_purchased'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members = 
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider = 
		LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
		USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */        
purchased_filtered = FILTER dq_purchased BY edh_bus_date == '$EDH_BUS_DATE';


/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
gen_dq_purchased_filtered_check_isnull = FOREACH purchased_filtered
										   GENERATE id AS id,
										   			est_sent_at AS est_sent_at,
										   			(service_provider_spid IS NULL OR (CHARARRAY)service_provider_spid =='' ? -1 : NULL) AS legacy_spid:INT,
										   			(service_provider_spid IS NULL OR (CHARARRAY)service_provider_spid =='' ? -1 : (INT)service_provider_spid) AS new_world_spid:INT,
													(INT)deal_info_sku_id AS source_ak:INT,
										   			(user_id IS NULL OR user_id =='' ? -1 : (INT)user_id) AS user_id,
                                					(user_id IS NULL OR user_id ==''? -1 : NULL ) AS member_id;

/* Split into 4 separate relations the records with user_id missing and user_id available */
SPLIT gen_dq_purchased_filtered_check_isnull INTO
                    table_dq_purchased_missing_user_id_missing IF user_id == -1,
                    table_dq_purchased_missing_user_id_available IF user_id != -1;


gen_table_dq_purchased_missing_user_id_available = FOREACH (JOIN table_dq_purchased_missing_user_id_available BY (INT)user_id LEFT OUTER, dim_members BY user_id ) 
                            GENERATE table_dq_purchased_missing_user_id_available::id AS id,  
                                     table_dq_purchased_missing_user_id_available::est_sent_at AS est_sent_at,
									 table_dq_purchased_missing_user_id_available::legacy_spid AS legacy_spid,
									 table_dq_purchased_missing_user_id_available::new_world_spid AS new_world_spid,
									 table_dq_purchased_missing_user_id_available::source_ak AS source_ak,
                                     (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                     (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;
									 

union_gen_table_dq_purchased_user_id_member_id = union table_dq_purchased_missing_user_id_missing ,
													 gen_table_dq_purchased_missing_user_id_available;
									 
/* Split into 2 seperate relations the records with spid missing and spid available */
SPLIT union_gen_table_dq_purchased_user_id_member_id INTO
					table_dim_service_provider_spid_missing IF new_world_spid == -1,
					table_dim_service_provider_spid_available IF new_world_spid != -1;
					
gen_table_dim_service_provider_spid_available = FOREACH (JOIN table_dim_service_provider_spid_available BY new_world_spid LEFT OUTER, dim_service_provider BY new_world_spid )
												 GENERATE table_dim_service_provider_spid_available::id AS id,
												          table_dim_service_provider_spid_available::est_sent_at AS est_sent_at,
														  (dim_service_provider::legacy_spid is null ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
														  (dim_service_provider::new_world_spid is null ? -3 : dim_service_provider::new_world_spid) AS new_world_spid,
														  table_dim_service_provider_spid_available::source_ak AS source_ak,
														  table_dim_service_provider_spid_available::user_id AS user_id,
														  table_dim_service_provider_spid_available::member_id AS member_id;
														  
union_gen_table_purchased_user_id_member_id_spid = UNION table_dim_service_provider_spid_missing, gen_table_dim_service_provider_spid_available;
   

tf_segment_purchased = FOREACH union_gen_table_purchased_user_id_member_id_spid 
					 GENERATE   id AS id, 
								(INT)(ToString(est_sent_at,'yyyyMMdd')) AS date_ak,
								ToString(est_sent_at,'HH:mm') AS time_ak,
								legacy_spid AS legacy_spid,
								new_world_spid AS new_world_spid,
								source_ak AS source_ak,
								'$TF_EVENT_SOURCE_TABLE' AS source_table,
								member_id AS member_id,
								user_id  AS user_id,
								$NUMERIC_NA_KEY AS category_id,
								'$TF_EVENT_NAME' AS event_type,
								'$STRING_NA_VALUE' AS search_type,
								$EVENT_SOURCE_WEB AS event_source:int,
								'$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
								null AS search_text:CHARARRAY,
								$TF_EVENT_QTY AS qty,
								$TF_EVENT_KEY AS event_type_key;
     
STORE tf_segment_purchased 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();