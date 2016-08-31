/*
PIG SCRIPT    : tf_fact_wm_22_segment_purchased_an_item.pig
AUTHOR        : Varun Rauthan
DATE          : 31 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event purchased_an_item from web Segment Source
*/


dq_purchased_an_item = 
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_purchased_an_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members = 
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider = 
		LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
		USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */        
purchased_an_item_filtered = FILTER dq_purchased_an_item BY edh_bus_date == '$EDH_BUS_DATE';


/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
gen_dq_purchased_an_item_filtered_check_isnull = FOREACH purchased_an_item_filtered
										   GENERATE id AS id,
										   			est_sent_at AS est_sent_at,
										   			(spid IS NULL OR (CHARARRAY)spid =='' ? -1 : NULL) AS legacy_spid:INT,
										   			(spid IS NULL OR (CHARARRAY)spid =='' ? -1 : (INT)spid) AS new_world_spid:INT,
													(INT)sku_id AS source_ak:INT,
										   			(user_id IS NULL OR user_id =='' ? -1 : (INT)user_id) AS user_id,
                                					(user_id IS NULL OR user_id ==''? -1 : NULL ) AS member_id,
										   			(sku_category IS NULL OR (CHARARRAY)sku_category =='' ? -1 : (INT)sku_category) AS category_id:INT;

/* Split into 4 separate relations the records with user_id missing and user_id available */
SPLIT gen_dq_purchased_an_item_filtered_check_isnull INTO
                    table_dq_purchased_an_item_missing_user_id_missing IF user_id == -1,
                    table_dq_purchased_an_item_missing_user_id_available IF user_id != -1;


gen_table_dq_purchased_an_item_missing_user_id_available = FOREACH (JOIN table_dq_purchased_an_item_missing_user_id_available BY (INT)user_id LEFT OUTER, dim_members BY user_id ) 
                            GENERATE table_dq_purchased_an_item_missing_user_id_available::id AS id,  
                                     table_dq_purchased_an_item_missing_user_id_available::est_sent_at AS est_sent_at,
									 table_dq_purchased_an_item_missing_user_id_available::legacy_spid AS legacy_spid,
									 table_dq_purchased_an_item_missing_user_id_available::new_world_spid AS new_world_spid,
									 table_dq_purchased_an_item_missing_user_id_available::source_ak AS source_ak,
                                     (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                     (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id,
									 table_dq_purchased_an_item_missing_user_id_available::category_id AS category_id;
									 

union_gen_table_dq_purchased_an_item_user_id_member_id = union table_dq_purchased_an_item_missing_user_id_missing ,
													 gen_table_dq_purchased_an_item_missing_user_id_available;
									 
/* Split into 2 seperate relations the records with spid missing and spid available */
SPLIT union_gen_table_dq_purchased_an_item_user_id_member_id INTO
					table_dim_service_provider_spid_missing IF new_world_spid == -1,
					table_dim_service_provider_spid_available IF new_world_spid != -1;
					
gen_table_dim_service_provider_spid_available = FOREACH (JOIN table_dim_service_provider_spid_available BY new_world_spid LEFT OUTER, dim_service_provider BY new_world_spid )
												 GENERATE table_dim_service_provider_spid_available::id AS id,
												          table_dim_service_provider_spid_available::est_sent_at AS est_sent_at,
														  (dim_service_provider::legacy_spid is null ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
														  (dim_service_provider::new_world_spid is null ? -3 : dim_service_provider::new_world_spid) AS new_world_spid,
														  table_dim_service_provider_spid_available::source_ak AS source_ak,
														  table_dim_service_provider_spid_available::user_id AS user_id,
														  table_dim_service_provider_spid_available::member_id AS member_id,
														  table_dim_service_provider_spid_available::category_id AS category_id;
														  
union_gen_table_purchased_an_item_user_id_member_id_spid_category_id = UNION table_dim_service_provider_spid_missing, gen_table_dim_service_provider_spid_available;
   

tf_segment_purchased_an_item = FOREACH union_gen_table_purchased_an_item_user_id_member_id_spid_category_id 
					 GENERATE   id AS id, 
								(INT)(ToString(est_sent_at,'YYYYMMDD')) AS date_ak,
								ToString(est_sent_at,'hh:mm') AS time_ak,
								legacy_spid AS legacy_spid,
								new_world_spid AS new_world_spid,
								source_ak AS source_ak,
								'$TF_EVENT_SOURCE_TABLE' AS source_table,
								member_id AS member_id,
								user_id  AS user_id,
								category_id AS category_id,
								'$TF_EVENT_NAME' AS event_type,
								'$STRING_NA_VALUE' AS search_type,
								$EVENT_SOURCE_WEB AS event_source:int,
								'$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
								null AS search_text:CHARARRAY,
								$TF_EVENT_QTY AS qty,
								$TF_EVENT_KEY AS event_type_key;
     
STORE tf_segment_purchased_an_item 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();