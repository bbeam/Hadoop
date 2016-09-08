/*
PIG SCRIPT    : tf_fact_wm_7_8_9_legacy_vw_addtcrt_prchs.pig
AUTHOR        : Anil Aleppy
DATE          : 02 Sep 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event View,Add To Cart And Purchase from Legacy Source
*/

/* Load Required Tables */
storefront_product_event = 
        LOAD '$GOLD_LEGACY_ANGIE_ANALYTICS_DBO_DB.dq_storefront_product_event'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
storefront_product_event_type = 
        LOAD '$GOLD_LEGACY_ANGIE_ANALYTICS_DBO_DB.dq_storefront_product_event_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
storefront_item = 
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();		
		
dim_members =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
dim_service_provider =
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


/* Filter by edh_bus_date to process records for previous day only. Apply other filters as per mapping logic  */
fltr_sfpe = FILTER storefront_product_event BY edh_bus_date == '$EDH_BUS_DATE';

/* Select out only required columns from storefront_product_event*/
sfpe_sel = FOREACH storefront_product_event GENERATE
                                            storefront_product_event_id AS storefront_product_event_id,
											storefront_product_event_type_id AS storefront_product_event_type_id,
											storefront_item_id AS storefront_item_id,
											member_id AS member_id,											
											est_create_date AS est_create_date;
/*Inner Join sfpe_sel with storefront_product_event_type and select storefront_product_event_type_description from storefront_product_event_type will be used as event_type*/
jn_sfpe_inn_sfpet = FOREACH (JOIN sfpe_sel BY storefront_product_event_type_id,storefront_product_event_type BY storefront_product_event_type_id)
                            GENERATE        sfpe_sel::storefront_product_event_id AS storefront_product_event_id,
											sfpe_sel::storefront_item_id AS storefront_item_id,
											storefront_product_event_type::storefront_product_event_type_description AS storefront_product_event_type_description,
											sfpe_sel::member_id AS member_id,											
											sfpe_sel::est_create_date AS est_create_date;
											
/*Inner Join jn_sfpe_inn_sfpet with storefront_item and select sp_id from storefront_item will be used as legacy_spid*/
jn_sfpe_inn_sfi = FOREACH (JOIN jn_sfpe_inn_sfpet BY storefront_item_id,storefront_item BY storefront_item_id)
                           GENERATE         jn_sfpe_inn_sfpet::storefront_product_event_id AS storefront_product_event_id,
											storefront_item::storefront_item_id AS storefront_item_id,
											jn_sfpe_inn_sfpet::storefront_product_event_type_description AS storefront_product_event_type_description,
											jn_sfpe_inn_sfpet::member_id AS member_id,
                                            storefront_item::sp_id AS legacy_spid,										
											jn_sfpe_inn_sfpet::est_create_date AS est_create_date;

/*Filter only the select events*/                                     								
sfp_sel = FILTER jn_sfpe_inn_sfi BY storefront_product_event_type_description IN ('View','Add to Cart','Purchase');
	
/* Check if member_id is null. If member id is null then populate both member_id and user_id as missing */
sfp_sel_check_member_id = FOREACH sfp_sel GENERATE
                                (chararray)storefront_product_event_id AS id,
                                est_create_date AS est_create_date,
								storefront_product_event_type_description AS event_type,
						        (storefront_item_id IS NULL OR (CHARARRAY)storefront_item_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)storefront_item_id) AS (source_ak:INT),
								legacy_spid AS legacy_spid,
							    (member_id IS NULL OR (CHARARRAY)member_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (user_id:INT),
						        (member_id IS NULL OR (CHARARRAY)member_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)member_id) AS (member_id:INT);

/* Split into 2 separate relations the records with member_id missing and those with member_id available */
SPLIT sfp_sel_check_member_id INTO
                    sfp_member_id_missing IF (member_id == $NUMERIC_MISSING_KEY),
                    sfp_member_id_available IF (member_id != $NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_sfp_member_id_available_members = FOREACH (JOIN sfp_member_id_available BY member_id LEFT , dim_members BY member_id ) 
                         GENERATE   sfp_member_id_available::id AS id,
                                    sfp_member_id_available::est_create_date AS est_create_date,
									sfp_member_id_available::event_type AS event_type,
									sfp_member_id_available::source_ak AS source_ak,
									sfp_member_id_available::legacy_spid AS legacy_spid,
                                    dim_members::user_id AS user_id,
									dim_members::member_id AS member_id;

/* Combine the 2 relations one with missing member_id and the other with member_id available */
un_sfp_members = UNION sfp_member_id_missing, jn_sfp_member_id_available_members;

/* Check if legacy_spid is null. If sp_id is null then populate both legacy_spid and new_world_spid as missing */
                                    
un_sfp_members_check_sp_id = FOREACH un_sfp_members GENERATE
                                id AS id,
                                est_create_date AS est_create_date,
                                event_type AS event_type,
                                source_ak AS source_ak,
                                (legacy_spid IS NULL ? $NUMERIC_MISSING_KEY : legacy_spid ) AS legacy_spid,
                                (legacy_spid IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS new_world_spid,
								 user_id AS user_id,
								member_id AS member_id;
								
/* Split into 2 separate relations the records with legacy_spid missing and those with legacy_spid available */

SPLIT un_sfp_members_check_sp_id INTO
                    sfp_members_legacy_spid_missing IF (legacy_spid == $NUMERIC_MISSING_KEY),
                    sfp_members_legacy_spid_available IF (legacy_spid != $NUMERIC_MISSING_KEY);
                                
/* Join with service_provider table to get the corresponding new_world_spid for a given legacy_spid */

jn_sfp_members_legacy_spid_available = FOREACH (JOIN un_sfp_members_check_sp_id BY legacy_spid LEFT , dim_service_provider BY legacy_spid ) 
                         GENERATE   un_sfp_members_check_sp_id::id AS id,
                                    un_sfp_members_check_sp_id::est_create_date AS est_create_date,
									un_sfp_members_check_sp_id::event_type AS event_type,
									un_sfp_members_check_sp_id::source_ak AS source_ak,
									dim_service_provider::legacy_spid AS legacy_spid,
									dim_service_provider::new_world_spid AS new_world_spid,									
                                    un_sfp_members_check_sp_id::user_id AS user_id,
									un_sfp_members_check_sp_id::member_id AS member_id;

/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
un_sfp_members_sp = UNION sfp_members_legacy_spid_missing, jn_sfp_members_legacy_spid_available;

                                    

/* Format the record as per the Target Table structure */
tf_sfp = FOREACH un_sfp_members_sp 
    GENERATE    
             (CHARARRAY)id AS (id:CHARARRAY),
              ToDate(ToString(est_create_date,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
              ToString(est_create_date,'HH:mm') AS (time_ak:chararray),
              (legacy_spid IS NULL ? $NUMERIC_UNKOWN_KEY :legacy_spid) AS legacy_spid,
              (new_world_spid IS NULL ? $NUMERIC_UNKOWN_KEY : new_world_spid) AS new_world_spid,
             (INT)source_ak AS (source_ak:int),
             (CHARARRAY)'StorefrontItem' AS (source_table:chararray),
             (member_id IS NULL?(INT)$NUMERIC_UNKOWN_KEY: member_id) AS (member_id:INT),
             (user_id IS NULL?(INT)$NUMERIC_UNKOWN_KEY:user_id) AS (user_id:INT),
             (INT)$NUMERIC_NA_KEY AS (category_id:INT),
             (event_type IS NULL?'$STRING_NA_VALUE':event_type)AS (event_type:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_type:CHARARRAY),
             (INT)$EVENT_SOURCE_WEB AS (event_source:INT),
             (CHARARRAY)'$EVENT_SUB_SOURCE_WEB' AS (event_sub_source:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_text:CHARARRAY),
             (INT)1 AS (qty:INT),
			 (event_type=='View'?'7':(event_type=='Add to Cart'?'8':(event_type=='Purchase'?'9':'$STRING_NA_VALUE'))) AS (event_type_key:CHARARRAY);
				
				
/* Store Data into target table */
STORE tf_sfp
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();