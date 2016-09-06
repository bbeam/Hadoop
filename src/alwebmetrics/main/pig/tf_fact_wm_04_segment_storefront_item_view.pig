/*
PIG SCRIPT    : tf_fact_wm_04_segment_storefront_item_view.pig
AUTHOR        : Varun Rauthan
DATE          : 06 Sep 16 
DESCRIPTION   : Data Transformation script for segment table for the event storefront_item_view from web segment source
*/


dq_storefront_item = 
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_storefront_details_view = 
		LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_details_view'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_members = 
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider = 
		LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
		USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */        
dq_storefront_details_view_filtered = FILTER dq_storefront_details_view BY member_id IS NOT NULL AND ToString(est_create_date,'yyyy-MM-dd') == '$EDH_BUS_DATE';


/* Join dq_report with dq_report_sp_category to get spid and category_id */
join_dq_storefront_item_with_dq_storefront_details_view = 
		JOIN dq_storefront_item by storefront_item_id, dq_storefront_details_view_filtered by store_front_item_id;


/* Check if user_id is null. If user id is null then populate both member_id and user_id as missing */
gen_join_dq_storefront_item_with_dq_storefront_details_view = 
										   FOREACH join_dq_storefront_item_with_dq_storefront_details_view
										   GENERATE dq_storefront_item::storefront_item_id AS id,
										   			dq_storefront_details_view_filtered::est_create_date AS est_create_date,
										   			(sp_id IS NULL OR (CHARARRAY)sp_id =='' ? -1 : (INT)sp_id) AS legacy_spid:INT,
										   			(sp_id IS NULL OR (CHARARRAY)sp_id =='' ? -1 : NULL) AS new_world_spid:INT,
													dq_storefront_item::storefront_item_id AS source_ak:INT,
													(dq_storefront_details_view_filtered::member_id IS NULL OR (CHARARRAY)dq_storefront_details_view_filtered::member_id =='' ? -1 : (INT)dq_storefront_details_view_filtered::member_id) AS member_id,
                                					(dq_storefront_details_view_filtered::member_id IS NULL OR (CHARARRAY)dq_storefront_details_view_filtered::member_id =='' ? -1 : NULL ) AS user_id;

/* Split into 4 separate relations the records with user_id missing and user_id available */
SPLIT gen_join_dq_storefront_item_with_dq_storefront_details_view INTO
                    table_join_storefront_missing_member_id_missing IF member_id == -1,
                    table_join_storefront_missing_member_id_available IF member_id != -1;


gen_table_join_storefront_missing_member_id_available = 
							FOREACH (JOIN table_join_storefront_missing_member_id_available BY (INT)member_id LEFT OUTER, dim_members BY member_id ) 
                            GENERATE table_join_storefront_missing_member_id_available::id AS id,  
                                     table_join_storefront_missing_member_id_available::est_create_date AS est_create_date,
									 table_join_storefront_missing_member_id_available::legacy_spid AS legacy_spid,
									 table_join_storefront_missing_member_id_available::new_world_spid AS new_world_spid,
									 table_join_storefront_missing_member_id_available::source_ak AS source_ak,
                                     (dim_members::user_id IS NULL ? -3 : dim_members::user_id) AS user_id,
                                     (dim_members::member_id IS NULL ? -3 : dim_members::member_id) AS member_id;
									 

union_gen_join_storefront_user_id_member_id = union table_join_storefront_missing_member_id_missing ,
													 gen_table_join_storefront_missing_member_id_available;
									 
/* Split into 2 seperate relations the records with spid missing and spid available */
SPLIT union_gen_join_storefront_user_id_member_id INTO
					table_join_storefront_sp_id_missing IF legacy_spid == -1,
					table_join_storefront_sp_id_available IF legacy_spid != -1;
					
gen_table_join_storefront_sp_id_available = FOREACH (JOIN table_join_storefront_sp_id_available BY legacy_spid LEFT OUTER, dim_service_provider BY legacy_spid )
										 GENERATE table_join_storefront_sp_id_available::id AS id,
												  table_join_storefront_sp_id_available::est_create_date AS est_create_date,
												  (dim_service_provider::legacy_spid is null ? -3 : dim_service_provider::legacy_spid) AS legacy_spid,
												  (dim_service_provider::new_world_spid is null ? -3 : dim_service_provider::new_world_spid) AS new_world_spid,
												  table_join_storefront_sp_id_available::source_ak AS source_ak,
												  table_join_storefront_sp_id_available::user_id AS user_id,
												  table_join_storefront_sp_id_available::member_id AS member_id;
												  
union_gen_join_storefront_user_id_member_id_spid = UNION table_join_storefront_sp_id_missing, gen_table_join_storefront_sp_id_available;


tf_storefront = FOREACH union_gen_join_storefront_user_id_member_id_spid 
					 GENERATE   (CHARARRAY)id AS id, 
					 			ToDate(ToString(est_create_date,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
                				ToString(est_create_date,'HH:mm') AS time_ak,
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
     
STORE tf_storefront 
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();