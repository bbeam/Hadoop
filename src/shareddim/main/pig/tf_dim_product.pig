/*
PIG SCRIPT    : tf_dim_category.pig
AUTHOR        : Anil Aleppy
DATE          : 18 Aug 16 
DESCRIPTION   : Data Transformation script for dim_product dimension
*/

/* ******************Processing legacy_storefront_item *************************/

/* Read Input Tables */
legacy_storefront_item          =  LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item'         USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_storefront_item_type     =  LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item_type'    USING org.apache.hive.hcatalog.pig.HCatLoader();


/* legacy_storefront_item = filter legacy_storefront_item  by storefront_item_id > 10000 and storefront_item_id  < 11000;*/

/*Getting item_type for each storefront_item record by joining with storefront_item_type table  */
storefront_item_join_storefront_item_type_tf = FOREACH (JOIN legacy_storefront_item BY storefront_item_type_id , 
                        legacy_storefront_item_type BY storefront_item_type_id)
                        GENERATE legacy_storefront_item::storefront_item_id AS source_ak,
                        'StorefrontItem' AS source_table,
                        'StorefrontItemID' AS source_column,
                        (legacy_storefront_item_type::storefront_item_type_id==6 AND 
                        ToString(legacy_storefront_item::create_date,'yyyy-MM-dd') >= '2015-01-10' ? 'LeadFeed': 'E-Commerce') AS master_product_group,
                        legacy_storefront_item_type::storefront_item_type_name AS product_type,
                        legacy_storefront_item::title AS product,
                        legacy_storefront_item::member_price AS unit_price,
                        'Legacy' AS source,
                        ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
                        ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;                        

/* ******************  Processing al_t_sku  *************************/

/* Read Input Tables */
alweb_t_sku                     = LOAD '$GOLD_ALWEB_AL_DB.dq_t_sku'                                 USING org.apache.hive.hcatalog.pig.HCatLoader(); 
alweb_t_sku_item                = LOAD '$GOLD_ALWEB_AL_DB.dq_t_sku_item'                            USING org.apache.hive.hcatalog.pig.HCatLoader();

/* alweb_t_sku = filter alweb_t_sku  by sku_id > 20000 and sku_id  < 90000;
alweb_t_sku_item = filter alweb_t_sku_item  by sku_id > 20000 and sku_id  < 90000; */

/*Getting member price for each t_sku record by joining with t_sku_item table*/
t_sku_join_t_sku_item_tf = FOREACH (JOIN alweb_t_sku BY sku_id , 
                        alweb_t_sku_item BY sku_id)
                        GENERATE alweb_t_sku::sku_id AS source_ak,
                        't_sku' AS source_table,
                        'SkuID' AS source_column,
                        'E-Commerce' AS master_product_group,
                        (alweb_t_sku::is_email_promotable == 1 ? 'BigDeal' : 'Storefront')  AS product_type,
                        alweb_t_sku::title AS product,
                        alweb_t_sku_item::member_price AS unit_price,
                        'AL4.0' AS source,
                        ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
                        ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;
						
/* There are more than one records for a sku_id in t_sku_item table. Hence we need to remvoe the duplicates*/
t_sku_join_t_sku_item_tf_distinct = DISTINCT t_sku_join_t_sku_item_tf;

/* STORE t_sku_join_t_sku_item_tf INTO 'work_shared_dim.tf_dim_product' USING org.apache.hive.hcatalog.pig.HCatStorer();*/

/* ******************  Processing al_lead  *************************/

/* Read Input Tables */
al_lead                     =  LOAD '$GOLD_ALWEB_FULFILLMENT_DB.dq_lead'                              USING org.apache.hive.hcatalog.pig.HCatLoader(); 

/*al_lead = filter al_lead  by   lead_id  > 0 and lead_id   < 1000;*/

al_lead_tf = FOREACH al_lead
                        GENERATE lead_id AS source_ak,
                        'Lead' AS source_table,
                        'LeadID' AS source_column,
                        'LeadFeed'  AS master_product_group,
                        'LeadFeed'  AS product_type,
                        title AS product,
                        price AS unit_price,
                        'AL4.0' AS source,
                        ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
                        ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;


/* STORE al_lead_tf INTO 'work_shared_dim.tf_dim_product' USING org.apache.hive.hcatalog.pig.HCatStorer(); */


/* ******************  Processing legacy_product  *************************/

/* Read Input Tables */
legacy_product              =  LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_product'                      USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_product_type         =  LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_product_type'                 USING org.apache.hive.hcatalog.pig.HCatLoader(); 

/*Getting product_type_name by joining with product_type table */
product_join_product_type_tf = FOREACH (JOIN legacy_product BY   product_type_id , 
                        legacy_product_type BY product_type_id)
                        GENERATE legacy_product::product_id AS source_ak,
                        'Product' AS source_table,
                        'ProductID' AS source_column,
                        'Membership' AS master_product_group,
                        legacy_product_type::product_type_name AS product_type,
                        legacy_product::product_name AS product,
                        (BIGDECIMAL) '0.00' AS unit_price,
                        'Legacy' AS source,
                        ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
                        ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;
						

/* STORE product_join_product_type_tf INTO 'work_shared_dim.tf_dim_product' USING org.apache.hive.hcatalog.pig.HCatStorer();*/

/* ******************  Processing ad_element  *************************/

/* Read Input Tables */
legacy_ad_element           =  LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_ad_element'                   USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_ad_type              =  LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_ad_type'                      USING org.apache.hive.hcatalog.pig.HCatLoader();   

legacy_ad_element_filtered = filter legacy_ad_element  by   ad_element_active == 1;

/*Getting member price for each sku record */
ad_element_join_ad_type_tf = FOREACH (JOIN legacy_ad_element_filtered  BY   ad_type_id , 
                        legacy_ad_type  BY ad_type_id)
                        GENERATE legacy_ad_element_filtered::ad_element_id AS source_ak,
                        'AdElement' AS source_table,
                        'AdElementID' AS source_column,
                        'AdvertisingContract' AS master_product_group,
                        legacy_ad_type::ad_type_name AS product_type,
                        legacy_ad_element_filtered::ad_element_name AS product,
                        legacy_ad_element_filtered::default_price AS unit_price,
                        'Legacy' AS source,
                        ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') AS est_load_timestamp,
                        ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') AS utc_load_timestamp;

/* STORE ad_element_join_ad_type_tf  INTO 'work_shared_dim.tf_dim_product' USING org.apache.hive.hcatalog.pig.HCatStorer();*/

/* Combine extracts from various tables above*/
dim_product_tf = UNION storefront_item_join_storefront_item_type_tf, t_sku_join_t_sku_item_tf_distinct, al_lead_tf,
                    product_join_product_type_tf,  ad_element_join_ad_type_tf; 
                    
/* Write to Target Work Table*/
STORE dim_product_tf  INTO '$WORK_SHARED_DIM_DB.tf_dim_product' USING org.apache.hive.hcatalog.pig.HCatStorer();

