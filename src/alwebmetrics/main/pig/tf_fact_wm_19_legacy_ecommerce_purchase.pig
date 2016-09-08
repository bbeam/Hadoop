/*
PIG SCRIPT    : tf_fact_wm_19_legacy_ecommerce_purchase.pig
AUTHOR        : Abhijeet Purwar
DATE          : 02 Spet 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event legacy_ecommerce_purchase from Segment Source
*/

dq_storefront_order_line_item =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_order_line_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_storefront_order =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_order'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_storefront_item =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_cash_posting =
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_cash_posting'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dq_exclude_test_sp_ids =
        LOAD '$GOLD_LEGACY_REPORTS_DBO_DB.dq_exclude_test_sp_ids'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_member =
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

dim_service_provider =
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

jn_storefront_order_line_item = FOREACH (JOIN dq_storefront_order_line_item BY storefront_item_id, dq_storefront_item BY storefront_item_id)
                                                 GENERATE dq_storefront_order_line_item::storefront_order_line_item_id AS storefront_order_line_item_id,
                                                          dq_storefront_order_line_item::storefront_order_id AS storefront_order_id,
                                                          dq_storefront_order_line_item::storefront_item_id AS storefront_item_id,
                                                          dq_storefront_order_line_item::cash_posting_id AS cash_posting_id,
                                                          dq_storefront_item::sp_id AS sp_id,
                                                          dq_storefront_item::storefront_item_type_id AS storefront_item_type_id;

exclude_test_sp_ids_null_fltr = FILTER dq_exclude_test_sp_ids BY sp_id is null;

jn_storefront_oli_etspid = FOREACH (JOIN jn_storefront_order_line_item BY sp_id LEFT, exclude_test_sp_ids_null_fltr BY sp_id)
                                                 GENERATE jn_storefront_order_line_item::storefront_order_line_item_id AS storefront_order_line_item_id,
                                                          jn_storefront_order_line_item::storefront_order_id AS storefront_order_id,
                                                          jn_storefront_order_line_item::storefront_item_id AS storefront_item_id,
                                                          jn_storefront_order_line_item::cash_posting_id AS cash_posting_id,
                                                          jn_storefront_order_line_item::sp_id AS sp_id,
                                                          jn_storefront_order_line_item::storefront_item_type_id AS storefront_item_type_id;
                                                          
jn_storefront_oli_cash_posting = FOREACH (JOIN jn_storefront_oli_etspid BY cash_posting_id, dq_cash_posting BY cash_posting_id)
                                                 GENERATE jn_storefront_oli_etspid::storefront_order_line_item_id AS storefront_order_line_item_id,
                                                          jn_storefront_oli_etspid::storefront_order_id AS storefront_order_id,
                                                          jn_storefront_oli_etspid::storefront_item_id AS storefront_item_id,
                                                          jn_storefront_oli_etspid::sp_id AS sp_id,
                                                          jn_storefront_oli_etspid::storefront_item_type_id AS storefront_item_type_id;

dq_storefront_order_fltr = FILTER dq_storefront_order BY ToString(create_date,'yyyy-MM-dd') == '$EDH_BUS_DATE';

storefront_order_line_item_stats_view = FOREACH (JOIN jn_storefront_oli_cash_posting BY storefront_order_id, dq_storefront_order_fltr BY storefront_order_id)
                                                 GENERATE jn_storefront_oli_cash_posting::storefront_order_line_item_id AS storefront_order_line_item_id,
                                                          jn_storefront_oli_cash_posting::storefront_order_id AS storefront_order_id,
                                                          jn_storefront_oli_cash_posting::storefront_item_id AS storefront_item_id,
                                                          jn_storefront_oli_cash_posting::sp_id AS sp_id,
                                                          jn_storefront_oli_cash_posting::storefront_item_type_id AS storefront_item_type_id,
                                                          dq_storefront_order_fltr::create_date AS purchase_date,
                                                          dq_storefront_order_fltr::member_id AS member_id;

/* Check if member_id is null. If member id is null then populate both member_id and user_id as missing */
storefront_order_line_item_stats_view_check_member_id = FOREACH storefront_order_line_item_stats_view GENERATE
                                storefront_order_line_item_id AS id,
                                purchase_date AS purchase_date,
                                sp_id AS legacy_spid,
                                storefront_item_id AS storefront_item_id,
                                (member_id IS NULL ? $NUMERIC_MISSING_KEY : (int) member_id) AS member_id,
                                (member_id IS NULL ? $NUMERIC_MISSING_KEY :  NULL ) AS user_id;

/* Split into 2 separate relations the records with member_id missing and those with member_id available */
SPLIT storefront_order_line_item_stats_view_check_member_id INTO
                    stats_view_member_id_missing IF (member_id == $NUMERIC_MISSING_KEY),
                    stats_view_member_id_available IF (member_id != $NUMERIC_MISSING_KEY);

/* Join with dim_member table to get the corresponding user_id for a given member_id */
jn_stats_view_member_id_available_members = FOREACH (JOIN stats_view_member_id_available BY member_id LEFT , dim_member BY member_id ) 
                         GENERATE   stats_view_member_id_available::id AS id,
                                    stats_view_member_id_available::purchase_date AS purchase_date,
                                    stats_view_member_id_available::legacy_spid AS legacy_spid,
                                    stats_view_member_id_available::storefront_item_id AS storefront_item_id,
                                    (dim_member::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_member::member_id) AS member_id,
                                    (dim_member::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : dim_member::user_id) AS user_id;

/* Combine the 2 relations one with missing member_id and the other with member_id available */
un_stats_view_members = UNION stats_view_member_id_missing, jn_stats_view_member_id_available_members;

/* Check if legacy_spid is null. If sp_id is null then populate both legacy_spid and new_world_spid as missing */
un_stats_view_members_check_sp_id = FOREACH un_stats_view_members GENERATE
                                id AS id,
                                purchase_date AS purchase_date,
                                storefront_item_id AS storefront_item_id,
                                member_id AS member_id,
                                user_id AS user_id,
                                (legacy_spid IS NULL ? $NUMERIC_MISSING_KEY : legacy_spid ) AS legacy_spid,
                                (legacy_spid IS NULL ? $NUMERIC_MISSING_KEY : NULL ) AS new_world_spid;

/* Split into 2 separate relations the records with legacy_spid missing and those with legacy_spid available */
SPLIT un_stats_view_members_check_sp_id INTO
                    stats_view_members_legacy_spid_missing IF (legacy_spid == $NUMERIC_MISSING_KEY),
                    stats_view_members_legacy_spid_available IF (legacy_spid != $NUMERIC_MISSING_KEY);

/* Join with service_provider table to get the corresponding new_world_spid for a given legacy_spid */
jn_stats_view_members_legacy_spid_available = FOREACH (JOIN stats_view_members_legacy_spid_available BY legacy_spid LEFT , dim_service_provider BY legacy_spid ) 
                         GENERATE   stats_view_members_legacy_spid_available::id AS id,
                                    stats_view_members_legacy_spid_available::purchase_date AS purchase_date,
                                    stats_view_members_legacy_spid_available::storefront_item_id AS storefront_item_id,
                                    stats_view_members_legacy_spid_available::member_id  AS member_id,
                                    stats_view_members_legacy_spid_available::user_id AS user_id,
                                    (dim_service_provider::legacy_spid IS NULL ? $NUMERIC_UNKOWN_KEY : dim_service_provider::legacy_spid) AS legacy_spid,
                                    (dim_service_provider::new_world_spid IS NULL ? $NUMERIC_UNKOWN_KEY : dim_service_provider::new_world_spid) AS new_world_spid;

/* Combine the 2 relations one with missing legacy_spid_id and the other with legacy_spid available */
un_stats_view_members_sp = UNION stats_view_members_legacy_spid_missing, jn_stats_view_members_legacy_spid_available;

/* Format the record as per the Target Table structure */
tf_legacy_ecommerce_purchase = FOREACH un_stats_view_members_sp 
    GENERATE    (chararray)id AS id,
                ToDate(ToString(purchase_date,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
                ToString(purchase_date,'HH:mm') AS time_ak,
                (int)legacy_spid AS legacy_spid,
                (int)new_world_spid AS new_world_spid,
                (int)storefront_item_id AS source_ak,
                (chararray)'$TF_ECOM_PURCHASE_SRC_TABLE' AS source_table,
                member_id AS member_id,
                user_id  AS user_id,
                (int)$NUMERIC_NA_KEY AS category_id,
                '$TF_EVENT_NAME' AS event_type,
                '$STRING_NA_VALUE' AS search_type,
                (int)$EVENT_SOURCE_WEB AS event_source:int,
                (chararray)'$EVENT_SUB_SOURCE_WEB' AS event_sub_source,
                (chararray)null AS search_text,
                (int)$TF_ECOM_PURCHASE_QTY AS qty,
                $TF_EVENT_KEY AS event_type_key;

/* Store Data into target table */
STORE tf_legacy_ecommerce_purchase
    INTO '$WORK_AL_WEB_METRICS_DB.tf_nk_fact_web_metrics'
    USING org.apache.hive.hcatalog.pig.HCatStorer();
