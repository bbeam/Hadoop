/*
PIG SCRIPT    : tf_service_provider.pig
AUTHOR        : Abhinav Mehar
DATE          : Tue Aug 16 
DESCRIPTION   : Data Transformation script for service_provider dimension
*/


/* Reading the input tables */
table_dq_service_provider=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


table_dq_t_service_provider=
        LOAD '$GOLD_ALWEB_AL_DB.dq_t_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_t_service_provider_address=
        LOAD '$GOLD_ALWEB_AL_DB.dq_t_service_provider_address'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dq_t_postal_address=
        LOAD '$GOLD_ALWEB_AL_DB.dq_t_postal_address'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_t_city=
        LOAD '$GOLD_ALWEB_AL_DB.dq_t_city'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dq_t_region=
        LOAD '$GOLD_ALWEB_AL_DB.dq_t_region'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        


table_dq_service_provider_group_association=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider_group_association'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

        
table_dq_service_provider_group=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider_group'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_service_provider_group_type=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider_group_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        
        
table_dq_contract_item=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_contract_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        


table_dq_ad_element=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_ad_element'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


table_dq_ad_type=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_ad_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_location=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_location'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        
        
table_dq_service_provider=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_service_provider_background_check=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider_background_check'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_service_provider_document=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_service_provider_document'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_storefront_item=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


table_dq_storefront_order=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_order'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dq_storefront_order_line_item=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_order_line_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_storefront_item=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_storefront_item'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_exclude_test_sp_ids=
        LOAD '$GOLD_LEGACY_REPORTS_DBO_DB.dq_exclude_test_sp_ids'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_storefront_item_history=
        LOAD '$GOLD_LEGACY_ANGIE_HISTORY_DBO_DB.dq_storefront_item_history'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        


table_dq_categories=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_categories'
        USING org.apache.hive.hcatalog.pig.HCatLoader();


table_dq_category_group=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_category_group'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
        
table_dq_category_group_type=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_category_group_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        


table_dq_ad_element=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_ad_element'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        
        
table_dq_contract=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_contract'
        USING org.apache.hive.hcatalog.pig.HCatLoader();        

table_dq_sp_market=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_sp_market'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_markets=
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_markets'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_dim_market=
        LOAD '$GOLD_SHARED_DIM_DB.dim_market'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

            
/* Start Process for vintage */
/* In this section vintage value 1,2 or 3 is getting derived for each sp_id*/

/* Select all the related columns from dq_contract_table and apply window condition to fetch data applicable up to todays date*/

sel_contract_item = FOREACH table_dq_contract_item GENERATE contract_id AS contract_id,
                                                            ad_element_id AS ad_element_id,
                                                            category_id AS category_id,
                                                            start_date AS start_date,
                                                            end_date AS end_date,
                                                            ToDate(ToString(ToDate(ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_var;

fltr_contract_item = FILTER sel_contract_item BY start_date <= todays_var and end_date >todays_var;                                                     

jn_wt_contract  = JOIN  table_dq_contract BY contract_id,fltr_contract_item BY contract_id;

sel_vintage_contract = FOREACH jn_wt_contract GENERATE table_dq_contract::sp_id AS sp_id,
                                                       table_dq_contract::contract_id AS contract_id,
													   table_dq_contract::publishing_department_id AS publishing_department_id,
                                                       fltr_contract_item::ad_element_id As ad_element_id,
                                                       fltr_contract_item::category_id AS category_id;
                                                       
jn_wt_category = JOIN sel_vintage_contract BY category_id LEFT OUTER,table_dq_categories BY category_id;            

sel_vintage_category = FOREACH jn_wt_category GENERATE sel_vintage_contract::sp_id AS sp_id,
                                                     sel_vintage_contract::ad_element_id AS ad_element_id,
													 sel_vintage_contract::publishing_department_id AS publishing_department_id,
													 table_dq_categories::category_group_id AS category_group_id;

jn_wt_category_group = JOIN sel_vintage_category BY category_group_id LEFT OUTER,table_dq_category_group BY category_group_id;
						
sel_vintage_category_group = FOREACH jn_wt_category_group GENERATE sel_vintage_category::sp_id AS sp_id,
                                                                   sel_vintage_category::ad_element_id AS ad_element_id,
																   sel_vintage_category::publishing_department_id AS publishing_department_id,
                                                                   sel_vintage_category::category_group_id AS category_group_id,
                                                                   table_dq_category_group::category_group_type_id AS category_group_type_id;
                                                                   
jn_wt_category_group_type = JOIN sel_vintage_category_group BY category_group_type_id,table_dq_category_group_type BY category_group_type_id;

sel_vintage_category_group_type = FOREACH jn_wt_category_group_type GENERATE 
                                                                   sel_vintage_category_group::sp_id AS sp_id,
                                                                   sel_vintage_category_group::ad_element_id AS ad_element_id,
																   sel_vintage_category_group::publishing_department_id AS publishing_department_id,
                                                                   table_dq_category_group_type::category_group_type_id AS category_group_type_id;

jn_wt_ad_element = JOIN sel_vintage_category_group_type BY ad_element_id LEFT OUTER, table_dq_ad_element BY ad_element_id;	

sel_vintage_ad_element = FOREACH jn_wt_ad_element GENERATE 	
                                                           sel_vintage_category_group_type::sp_id AS sp_id,
														   sel_vintage_category_group_type::publishing_department_id AS publishing_department_id,
                                                           sel_vintage_category_group_type::category_group_type_id AS category_group_type_id,
						                                   table_dq_ad_element::is_advertiser AS is_advertiser,
						                                   table_dq_ad_element::ad_type_id AS ad_type_id;														   

sel_vintage_null_check =  FOREACH sel_vintage_ad_element GENERATE 
                                                                 sp_id AS sp_id,
                                                                 (publishing_department_id IS NULL?0:publishing_department_id) AS publishing_department_id,
                                                                 (category_group_type_id IS NULL?0:category_group_type_id) AS category_group_type_id,
                                                                 (is_advertiser IS NULL?0:is_advertiser) AS is_advertiser;

sel_pre_vintage= FOREACH sel_vintage_null_check GENERATE
                                                   sp_id as sp_id, 
                                                   ((category_group_type_id==2 AND is_advertiser==1)?3:((publishing_department_id==10 AND is_advertiser==1)?2:1)) AS (pre_vintage:int);
grp_vintage= GROUP sel_pre_vintage BY sp_id;
												   
sel_vintage= FOREACH grp_vintage GENERATE group AS vin_sp_id,
                                          MAX(sel_pre_vintage.pre_vintage) AS vintage;
													 

/* Start Process for Advertisers */
/* In this section advertizer type will be identified for each of the sp_id based on the adtype location */

jn_wt_ad_type = JOIN sel_vintage_ad_element BY ad_type_id LEFT OUTER,table_dq_ad_type BY ad_type_id;

sel_ad_type = FOREACH jn_wt_ad_type GENERATE 
											sel_vintage_ad_element::sp_id AS sp_id,
                                            table_dq_ad_type::location_id AS location_id;

filter_table_dq_location = FILTER table_dq_location BY location_id in (1,2,3);											

join_location_wt_adtype= JOIN sel_ad_type BY location_id LEFT OUTER,filter_table_dq_location BY location_id;

sel_pre_advertiser = FOREACH join_location_wt_adtype GENERATE                   
                                                              sel_ad_type::sp_id AS sp_id,
                                                              ((filter_table_dq_location::location_id==1)?1:0) AS WebAdvertiser,
                                                              ((filter_table_dq_location::location_id==2)?1:0) AS CallCenterAdvertiser,
                                                              ((filter_table_dq_location::location_id==3)?1:0) AS PubAdvertiser;
grp_sel_pre_advertiser = GROUP sel_pre_advertiser BY sp_id;

sel_advertiser = FOREACH grp_sel_pre_advertiser GENERATE group as advertiser_sp_id,
        (SUM(sel_pre_advertiser.WebAdvertiser)>=1?1:0) AS WebAdvertiser,
        (SUM(sel_pre_advertiser.CallCenterAdvertiser)>=1?1:0) AS CallCenterAdvertiser,
        (SUM(sel_pre_advertiser.PubAdvertiser)>=1?1:0) AS PubAdvertiser;
        
/* Start Process for ServiceProviderDocument */
/* In this section based on the documnet type each sp_id will be marked as licensed,bonded and insured or not */
   
filter_table_dq_service_provider_document = FILTER table_dq_service_provider_document BY document_type_id in (1,2,3);


sel_pre_service_provider_document = FOREACH filter_table_dq_service_provider_document GENERATE sp_id as sp_id,
                                                              ((document_type_id==1)?1:0) AS IsLicensed,
                                                              ((document_type_id==2)?1:0) AS IsBonded,
                                                              ((document_type_id==3)?1:0) AS IsInsured;

grp_table_dq_service_provider_document = GROUP sel_pre_service_provider_document BY sp_id;
                                                              
                                                              
sel_service_provider_document = FOREACH grp_table_dq_service_provider_document GENERATE group as doc_sp_id,
        (SUM(sel_pre_service_provider_document.IsLicensed)>=1?1:0) AS IsLicensed,
        (SUM(sel_pre_service_provider_document.IsBonded)>=1?1:0) AS IsBonded,
        (SUM(sel_pre_service_provider_document.IsInsured)>=1?1:0) AS IsInsured;



/* Start Process for BackgroundCheck */
/* In this section based on the background_check_status_id background verified service providers are indentified */

filter_table_dq_service_provider_background_check = FILTER table_dq_service_provider_background_check BY background_check_status_id ==1;

grp_table_dq_service_provider_background_check = GROUP filter_table_dq_service_provider_background_check BY service_provider_id;

sel_service_provider_background_check = FOREACH grp_table_dq_service_provider_background_check GENERATE group as bg_check_sp_id, 
                                 1 AS BackgroundCheck;
								 
/* Start Process for ECommerceStatus */
/* In this section based on the ECommerceStatus od the service provides */

sel_storefront_order = FOREACH table_dq_storefront_order GENERATE 
                                                                storefront_order_id AS storefront_order_id,
                                                                create_date AS create_date,
                                                                ToDate(ToString(ToDate(ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_var,
                                                                ToDate(ToString(ToDate(ToString(AddDuration(CurrentTime(),'PT+24H'),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_plus_1_var;
																
filter_storefront_order = FILTER sel_storefront_order BY create_date>=todays_var AND create_date<todays_plus_1_var;

jn_wt_storefront_order_line_item = JOIN filter_storefront_order BY storefront_order_id,table_dq_storefront_order_line_item BY storefront_order_id;

sel_storefront_order_line_item = FOREACH jn_wt_storefront_order_line_item GENERATE 
                                                                                storefront_item_id AS storefront_item_id;

jn_storefront_line_item_wt_item = JOIN sel_storefront_order_line_item BY storefront_item_id,table_dq_storefront_item BY storefront_item_id;

sel_storefront_line_item_wt_item = FOREACH jn_storefront_line_item_wt_item GENERATE
                                                                                 sel_storefront_order_line_item::storefront_item_id AS storefront_item_id,  
                                                                                 sp_id AS sp_id;

jn_exclude_test_sp_ids = JOIN sel_storefront_line_item_wt_item	 BY sp_id LEFT OUTER,table_dq_exclude_test_sp_ids BY sp_id;	

sel_exclude_test_sp_ids = FOREACH jn_exclude_test_sp_ids GENERATE 
                                                                 sel_storefront_line_item_wt_item::storefront_item_id AS storefront_item_id,
                                                                 table_dq_exclude_test_sp_ids::sp_id AS exclude_sp_ids;
																 
fltr_exclude_test_sp_ids =  FILTER sel_exclude_test_sp_ids BY exclude_sp_ids IS NULL;

sel_sp_id1 = FOREACH fltr_exclude_test_sp_ids GENERATE 	
                                                       storefront_item_id as storefront_item_id;
									 

sel_storefront_item_history = FOREACH table_dq_storefront_item_history GENERATE
                                                                               sp_id AS sp_id,
																			   storefront_item_status_id AS storefront_item_status_id,
																			   storefront_item_id AS storefront_item_id,
																			   start_datetime AS start_datetime,
																			   end_datetime AS end_datetime,
																			   history_date AS history_date,
																			   history_end_date AS history_end_date,
                                                                               ToDate(ToString(ToDate(ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_var,
                                                                               ToDate(ToString(ToDate(ToString(AddDuration(CurrentTime(),'PT+24H'),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_plus_1_var;

fltr_storefront_item_history = FILTER sel_storefront_item_history BY start_datetime>=todays_var AND end_datetime<todays_plus_1_var AND history_date >= todays_var AND  (history_end_date IS NULL?ToDate('2058-01-01'):history_end_date) < todays_plus_1_var AND storefront_item_status_id==2;	

jn_wt_excludetestspid = JOIN fltr_storefront_item_history BY sp_id LEFT OUTER,table_dq_exclude_test_sp_ids by sp_id;	

sel_excludetestspid	= FOREACH jn_wt_excludetestspid GENERATE 
                                                            fltr_storefront_item_history::sp_id as sp_id,
                                                            fltr_storefront_item_history::storefront_item_id AS storefront_item_id,
                                                            table_dq_exclude_test_sp_ids::sp_id AS exclude_sp_ids;
															
fltr_excludetestspid = FILTER sel_excludetestspid BY exclude_sp_ids IS NULL;

sel_sp_id2 = FOREACH fltr_excludetestspid GENERATE 
                                                  storefront_item_id AS storefront_item_id;

sel_inn_storefront_item = FOREACH table_dq_storefront_item GENERATE 
                                                                   sp_id AS sp_id,
                                                                   storefront_item_id AS storefront_item_id,
																   storefront_item_status_id AS storefront_item_status_id,
																   start_datetime AS start_datetime,
																   end_datetime AS end_datetime,
                                                                   ToDate(ToString(ToDate(ToString(CurrentTime(),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_var,
                                                                   ToDate(ToString(ToDate(ToString(AddDuration(CurrentTime(),'PT+24H'),'yyyy-MM-dd HH:mm:ss.SSSZ'),'yyyy-MM-dd HH:mm:ss.SSSZ','EST'),'yyyy-MM-dd'),'yyyy-MM-dd') AS todays_plus_1_var;

fltr_inn_storefront_item = FILTER sel_inn_storefront_item BY start_datetime>=todays_var AND end_datetime<todays_plus_1_var;

join_inn_excludetestspids = JOIN fltr_inn_storefront_item BY sp_id LEFT OUTER,table_dq_exclude_test_sp_ids by sp_id;

fltr_inn_excludetestspids = FILTER join_inn_excludetestspids BY table_dq_exclude_test_sp_ids::sp_id IS NULL;

sel_sp_id3 = FOREACH fltr_inn_excludetestspids GENERATE storefront_item_id AS storefront_item_id;

union_sp_id= UNION sel_sp_id1,sel_sp_id2,sel_sp_id3;

distinct_sp_id= DISTINCT union_sp_id;

join_storefront_item_distinct_sp_id = JOIN table_dq_storefront_item BY storefront_item_id,distinct_sp_id BY storefront_item_id;

sel_pre_ecommercestatus = FOREACH join_storefront_item_distinct_sp_id GENERATE table_dq_storefront_item::sp_id AS ecom_sp_id, 1 AS ecommercestatus;

sel_ecommercestatus = DISTINCT sel_pre_ecommercestatus;

/* Start Process for Al4 SP_ID */
/* Derive all the required attributes from AL4 system*/

/*Derive service_provider_id,al_id,company_name,joined_date, address, update date mapping*/
join_t_service_provider_wt_address = JOIN table_dq_t_service_provider BY service_provider_id LEFT OUTER,table_dq_t_service_provider_address BY service_provider_id;

sel_sp_address = FOREACH join_t_service_provider_wt_address GENERATE 
                                                                   table_dq_t_service_provider_address::update_date AS update_date,
                                                                   table_dq_t_service_provider::al_id AS al_id,
                                                                   table_dq_t_service_provider::service_provider_id AS sp_id,
                                                                   table_dq_t_service_provider::name AS company_name,
                                                                   table_dq_t_service_provider::joined_date As joined_date;

grp_sel_sp_address= GROUP sel_sp_address BY (al_id,sp_id,company_name,joined_date);

/*Derive max address update date to get the latest address associated with service provider*/

sel_pre_al4_attributes = FOREACH grp_sel_sp_address GENERATE 
                                                                             FLATTEN(group) AS (al_id,sp_id,company_name,joined_date),
																			 MAX(sel_sp_address.update_date) as update_date;

/*Derive latest postal address ,city and region associated with service provider*/

join_al4_wt_sp_address = JOIN sel_pre_al4_attributes BY (sp_id , update_date)LEFT OUTER,table_dq_t_service_provider_address BY (service_provider_id , update_date);

sel_sp_address2 = FOREACH join_al4_wt_sp_address GENERATE sel_pre_al4_attributes::sp_id AS sp_id,
                                                             sel_pre_al4_attributes::al_id AS al_id,
                                                             sel_pre_al4_attributes::company_name AS company_name,
                                                             sel_pre_al4_attributes::joined_date AS joined_date,
															 table_dq_t_service_provider_address::postal_address_id;
															 
join_al4_wt_postal_address = JOIN sel_sp_address2 BY (postal_address_id) LEFT OUTER,table_dq_t_postal_address BY (postal_address_id);

sel_postal_address = FOREACH join_al4_wt_postal_address GENERATE sel_sp_address2::sp_id AS sp_id,
                                                             sel_sp_address2::al_id AS al_id,
                                                             sel_sp_address2::company_name AS company_name,
                                                             sel_sp_address2::joined_date AS joined_date,
															 table_dq_t_postal_address::postal_code AS postal_code,
															 table_dq_t_postal_address::city_id AS city_id;

join_al4_wt_city = JOIN sel_postal_address BY city_id LEFT OUTER,table_dq_t_city BY city_id;


sel_city = FOREACH join_al4_wt_city GENERATE sel_postal_address::sp_id AS sp_id,
                                                             sel_postal_address::al_id AS al_id,
                                                             sel_postal_address::company_name AS company_name,
                                                             sel_postal_address::joined_date AS joined_date,
                                                             sel_postal_address::postal_code AS postal_code,
                                                             table_dq_t_city::name AS city,
															 table_dq_t_city::region_id AS region_id;
/*Derive abbreviation of region of each service provider corresponds to*/

join_al4_wt_region = JOIN sel_city BY region_id LEFT OUTER,table_dq_t_region BY region_id;


sel_al4_attributes = FOREACH join_al4_wt_region GENERATE 
                                                  sel_city::sp_id AS al4_sp_id,
                                                  sel_city::al_id AS al_id,
                                                  sel_city::company_name AS company_name,
                                                  sel_city::joined_date AS joined_date,
                                                  sel_city::city AS city,
                                                  table_dq_t_region::abbreviation AS abbreviation,
                                                  sel_city::postal_code AS postal_code;


/*Start process for market attributes*/
/* populates market_key of primary market of a spid. If there is no primary market defined for a spid then market_key will be -1 which is the dummy key*/
filter_table_dq_sp_market = FILTER table_dq_sp_market BY primary_market==1;

grp_sp_market = GROUP table_dq_sp_market BY sp_id;

sel_max_create_dt_sp_market = FOREACH grp_sp_market GENERATE group AS sp_id ,MAX(table_dq_sp_market.create_date) AS max_create_date;

join_sp_market_wt_max_create_dt= JOIN sel_max_create_dt_sp_market BY (sp_id,max_create_date),filter_table_dq_sp_market BY (sp_id,create_date);

join_dim_market_wt_sp_market = JOIN join_sp_market_wt_max_create_dt BY market_id FULL, table_dq_dim_market BY market_id;

sel_pre_market_attributes = FOREACH join_dim_market_wt_sp_market GENERATE join_sp_market_wt_max_create_dt::filter_table_dq_sp_market::sp_id AS sp_id,
                                             table_dq_dim_market::market_id AS market_id,
                                             table_dq_dim_market::market_key AS market_key;

filter_dim_market =  FILTER table_dq_dim_market BY market_id == -1; 
                                     
cross_join_dim_market = CROSS filter_dim_market,sel_pre_market_attributes;                                       

sel_market_attributes = FOREACH cross_join_dim_market GENERATE sel_pre_market_attributes::sp_id AS market_sp_id,
sel_pre_market_attributes::market_id AS market_id,
sel_pre_market_attributes::market_key AS market_key,
filter_dim_market::market_key AS cross_market_key;

/* Start Process to join gold_legacy_angie_dbo.dq_service_provider with all the lookup tables */

jn_lsp_wt_market = JOIN table_dq_service_provider BY sp_id LEFT OUTER,sel_market_attributes BY market_sp_id;

sel_market = FOREACH jn_lsp_wt_market GENERATE 
                                                table_dq_service_provider::sp_id AS sp_id,
												sel_market_attributes::market_id AS market_id,
												sel_market_attributes::cross_market_key AS cross_market_key,
												sel_market_attributes::market_key AS market_key,
												table_dq_service_provider::company_name AS company_name,
												table_dq_service_provider::entered_date AS entered_date,
												table_dq_service_provider::city AS legacy_city,
												table_dq_service_provider::state AS state,
												table_dq_service_provider::zip AS zip,
												table_dq_service_provider::exclude_reason AS exclude_reason;
												
jn_lsp_wt_sp_grp_asso = JOIN sel_market BY sp_id LEFT OUTER,table_dq_service_provider_group_association BY sp_id;

sel_sp_grp_asso = FOREACH jn_lsp_wt_sp_grp_asso GENERATE 
                                                sel_market::sp_id AS sp_id,
												sel_market::market_id AS market_id,
												sel_market::cross_market_key AS cross_market_key,
												sel_market::market_key AS market_key,
												sel_market::company_name AS company_name,
												sel_market::entered_date AS entered_date,
												sel_market::legacy_city AS legacy_city,
												sel_market::state AS state,
												sel_market::zip AS zip,
												sel_market::exclude_reason AS exclude_reason,
												table_dq_service_provider_group_association::service_provider_group_id AS service_provider_group_id;

												
												
jn_lsp_wt_sp_grp = JOIN sel_sp_grp_asso BY service_provider_group_id LEFT OUTER,table_dq_service_provider_group BY service_provider_group_id;

sel_sp_grp = FOREACH jn_lsp_wt_sp_grp GENERATE 
                                                sel_sp_grp_asso::sp_id AS sp_id,
												sel_sp_grp_asso::market_id AS market_id,
												sel_sp_grp_asso::cross_market_key AS cross_market_key,
												sel_sp_grp_asso::market_key AS market_key,
												sel_sp_grp_asso::company_name AS company_name,
												sel_sp_grp_asso::entered_date AS entered_date,
												sel_sp_grp_asso::legacy_city AS legacy_city,
												sel_sp_grp_asso::state AS state,
												sel_sp_grp_asso::zip AS zip,
												sel_sp_grp_asso::exclude_reason AS exclude_reason,
												table_dq_service_provider_group::service_provider_group_type_id AS service_provider_group_type_id;
                                          

jn_lsp_wt_sp_grp_type = JOIN sel_sp_grp  BY service_provider_group_type_id LEFT OUTER,table_dq_service_provider_group_type BY service_provider_group_type_id;

sel_sp_grp_type = FOREACH jn_lsp_wt_sp_grp_type GENERATE 
                                                sel_sp_grp::sp_id AS sp_id,
												sel_sp_grp::market_id AS market_id,
												sel_sp_grp::cross_market_key AS cross_market_key,
												sel_sp_grp::market_key AS market_key,
												sel_sp_grp::company_name AS company_name,
												sel_sp_grp::entered_date AS entered_date,
												sel_sp_grp::legacy_city AS legacy_city,
												sel_sp_grp::state AS state,
												sel_sp_grp::zip AS zip,
												sel_sp_grp::exclude_reason AS exclude_reason,
												table_dq_service_provider_group_type::service_provider_group_type_name AS service_provider_group_type_name;


join_lsp_wt_alsp = JOIN sel_sp_grp_type BY sp_id FULL OUTER,sel_al4_attributes BY al_id;

sel_lsp_alsp = FOREACH join_lsp_wt_alsp GENERATE 
                                                   sel_sp_grp_type::sp_id AS sp_id,
												   sel_sp_grp_type::market_id AS market_id,
												   sel_sp_grp_type::cross_market_key AS cross_market_key,
												   sel_sp_grp_type::market_key AS market_key,
												   sel_sp_grp_type::company_name AS company_name,
												   sel_sp_grp_type::entered_date AS entered_date,
												   sel_sp_grp_type::legacy_city AS legacy_city,
												   sel_sp_grp_type::state AS state,
												   sel_sp_grp_type::zip AS zip,
												   sel_sp_grp_type::exclude_reason AS exclude_reason,
											       sel_sp_grp_type::service_provider_group_type_name AS service_provider_group_type_name,
                                                   sel_al4_attributes::al4_sp_id AS al4_sp_id,
                                                   sel_al4_attributes::company_name AS al4_company_name,
                                                   sel_al4_attributes::joined_date AS joined_date,
                                                   sel_al4_attributes::city AS city,
                                                   sel_al4_attributes::abbreviation AS abbreviation,
                                                   sel_al4_attributes::postal_code AS postal_code;

jn_lsp_wt_vintage = JOIN sel_lsp_alsp BY sp_id LEFT OUTER,sel_vintage BY vin_sp_id;

sel_lsp_vintage = FOREACH jn_lsp_wt_vintage GENERATE 
                                                   sel_lsp_alsp::sp_id AS sp_id,
												   sel_lsp_alsp::market_id AS market_id,
												   sel_lsp_alsp::cross_market_key AS cross_market_key,
												   sel_lsp_alsp::market_key AS market_key,
												   sel_lsp_alsp::company_name AS company_name,
												   sel_lsp_alsp::entered_date AS entered_date,
												   sel_lsp_alsp::legacy_city AS legacy_city,
												   sel_lsp_alsp::state AS state,
												   sel_lsp_alsp::zip AS zip,
												   sel_lsp_alsp::exclude_reason AS exclude_reason,
											       sel_lsp_alsp::service_provider_group_type_name AS service_provider_group_type_name,
                                                   sel_lsp_alsp::al4_sp_id AS al4_sp_id,
                                                   sel_lsp_alsp::al4_company_name AS al4_company_name,
                                                   sel_lsp_alsp::joined_date AS joined_date,
                                                   sel_lsp_alsp::city AS city,
                                                   sel_lsp_alsp::abbreviation AS abbreviation,
                                                   sel_lsp_alsp::postal_code AS postal_code,
												   sel_vintage::vintage AS vintage;

												   
												   
jn_lsp_wt_advertiser= JOIN sel_lsp_vintage BY sp_id LEFT OUTER,sel_advertiser BY advertiser_sp_id;

sel_lsp_advertiser = FOREACH jn_lsp_wt_advertiser GENERATE 
                                                   sel_lsp_vintage::sp_id AS sp_id,
												   sel_lsp_vintage::market_id AS market_id,
												   sel_lsp_vintage::cross_market_key AS cross_market_key,
												   sel_lsp_vintage::market_key AS market_key,
												   sel_lsp_vintage::company_name AS company_name,
												   sel_lsp_vintage::entered_date AS entered_date,
												   sel_lsp_vintage::legacy_city AS legacy_city,
												   sel_lsp_vintage::state AS state,
												   sel_lsp_vintage::zip AS zip,
												   sel_lsp_vintage::exclude_reason AS exclude_reason,
											       sel_lsp_vintage::service_provider_group_type_name AS service_provider_group_type_name,
                                                   sel_lsp_vintage::al4_sp_id AS al4_sp_id,
                                                   sel_lsp_vintage::al4_company_name AS al4_company_name,
                                                   sel_lsp_vintage::joined_date AS joined_date,
                                                   sel_lsp_vintage::city AS city,
                                                   sel_lsp_vintage::abbreviation AS abbreviation,
                                                   sel_lsp_vintage::postal_code AS postal_code,
												   sel_lsp_vintage::vintage AS vintage,
												   sel_advertiser::WebAdvertiser AS WebAdvertiser,
                                                   sel_advertiser::CallCenterAdvertiser AS CallCenterAdvertiser,
												   sel_advertiser::PubAdvertiser AS PubAdvertiser;
												   
jn_lsp_wt_doctype= JOIN sel_lsp_advertiser BY sp_id LEFT OUTER,sel_service_provider_document BY doc_sp_id;

sel_lsp_doctype = FOREACH jn_lsp_wt_doctype GENERATE 
                                                   sel_lsp_advertiser::sp_id AS sp_id,
												   sel_lsp_advertiser::market_id AS market_id,
												   sel_lsp_advertiser::cross_market_key AS cross_market_key,
												   sel_lsp_advertiser::market_key AS market_key,
												   sel_lsp_advertiser::company_name AS company_name,
												   sel_lsp_advertiser::entered_date AS entered_date,
												   sel_lsp_advertiser::legacy_city AS legacy_city,
												   sel_lsp_advertiser::state AS state,
												   sel_lsp_advertiser::zip AS zip,
												   sel_lsp_advertiser::exclude_reason AS exclude_reason,
											       sel_lsp_advertiser::service_provider_group_type_name AS service_provider_group_type_name,
                                                   sel_lsp_advertiser::al4_sp_id AS al4_sp_id,
                                                   sel_lsp_advertiser::al4_company_name AS al4_company_name,
                                                   sel_lsp_advertiser::joined_date AS joined_date,
                                                   sel_lsp_advertiser::city AS city,
                                                   sel_lsp_advertiser::abbreviation AS abbreviation,
                                                   sel_lsp_advertiser::postal_code AS postal_code,
												   sel_lsp_advertiser::vintage AS vintage,
												   sel_lsp_advertiser::WebAdvertiser AS WebAdvertiser,
                                                   sel_lsp_advertiser::CallCenterAdvertiser AS CallCenterAdvertiser,
												   sel_lsp_advertiser::PubAdvertiser AS PubAdvertiser,
                                                   sel_service_provider_document::IsInsured AS IsInsured,
                                                   sel_service_provider_document::IsBonded AS IsBonded,
                                                   sel_service_provider_document::IsLicensed  AS IsLicensed;

jn_lsp_wt_backgroundcheck= JOIN sel_lsp_doctype BY sp_id LEFT OUTER,sel_service_provider_background_check BY bg_check_sp_id;

sel_lsp_backgroundcheck = FOREACH jn_lsp_wt_backgroundcheck GENERATE 
                                                   sel_lsp_doctype::sp_id AS sp_id,
												   sel_lsp_doctype::market_id AS market_id,
												   sel_lsp_doctype::cross_market_key AS cross_market_key,
												   sel_lsp_doctype::market_key AS market_key,
												   sel_lsp_doctype::company_name AS company_name,
												   sel_lsp_doctype::entered_date AS entered_date,
												   sel_lsp_doctype::legacy_city AS legacy_city,
												   sel_lsp_doctype::state AS state,
												   sel_lsp_doctype::zip AS zip,
												   sel_lsp_doctype::exclude_reason AS exclude_reason,
											       sel_lsp_doctype::service_provider_group_type_name AS service_provider_group_type_name,
                                                   sel_lsp_doctype::al4_sp_id AS al4_sp_id,
                                                   sel_lsp_doctype::al4_company_name AS al4_company_name,
                                                   sel_lsp_doctype::joined_date AS joined_date,
                                                   sel_lsp_doctype::city AS city,
                                                   sel_lsp_doctype::abbreviation AS abbreviation,
                                                   sel_lsp_doctype::postal_code AS postal_code,
												   sel_lsp_doctype::vintage AS vintage,
												   sel_lsp_doctype::WebAdvertiser AS WebAdvertiser,
                                                   sel_lsp_doctype::CallCenterAdvertiser AS CallCenterAdvertiser,
												   sel_lsp_doctype::PubAdvertiser AS PubAdvertiser,
                                                   sel_lsp_doctype::IsInsured AS IsInsured,
                                                   sel_lsp_doctype::IsBonded AS IsBonded,
                                                   sel_lsp_doctype::IsLicensed  AS IsLicensed,
                                                   sel_service_provider_background_check::BackgroundCheck as BackgroundCheck;
												   
jn_lsp_wt_ecommercestatus= JOIN sel_lsp_backgroundcheck BY sp_id LEFT OUTER,sel_ecommercestatus BY ecom_sp_id;


sel_lsp_ecommercestatus = FOREACH jn_lsp_wt_ecommercestatus GENERATE 
                                                   sel_lsp_backgroundcheck::sp_id AS legacy_spid,
                                                   sel_lsp_backgroundcheck::al4_sp_id AS new_world_spid,
												   sel_lsp_backgroundcheck::al4_company_name AS al4_company_name,
												   sel_lsp_backgroundcheck::company_name AS legacy_company_name,
											       sel_lsp_backgroundcheck::service_provider_group_type_name AS service_provider_group_type_name,
												   sel_lsp_backgroundcheck::joined_date AS al4_joined_date,
                                                   sel_lsp_backgroundcheck::entered_date AS leagcy_entered_date,
												   sel_lsp_backgroundcheck::legacy_city AS legacy_city,
												   sel_lsp_backgroundcheck::city AS al4_city,
												   sel_lsp_backgroundcheck::abbreviation AS al4_abbreviation,
												   sel_lsp_backgroundcheck::state AS legacy_state,
                                                   sel_lsp_backgroundcheck::postal_code AS al4_postal_code,
                                                   sel_lsp_backgroundcheck::zip AS legacy_zip,
                                                   sel_lsp_backgroundcheck::exclude_reason AS legacy_exclude_reason,
                                                   sel_lsp_backgroundcheck::WebAdvertiser AS WebAdvertiser,
                                                   sel_lsp_backgroundcheck::CallCenterAdvertiser AS CallCenterAdvertiser,
												   sel_lsp_backgroundcheck::PubAdvertiser AS PubAdvertiser,
                                                   sel_lsp_backgroundcheck::IsInsured AS IsInsured,
                                                   sel_lsp_backgroundcheck::IsBonded AS IsBonded,
                                                   sel_lsp_backgroundcheck::IsLicensed  AS IsLicensed,
                                                   sel_lsp_backgroundcheck::BackgroundCheck as BackgroundCheck,
												   sel_ecommercestatus::ecommercestatus AS ecommercestatus,
												   sel_lsp_backgroundcheck::vintage AS vintage,
												   sel_lsp_backgroundcheck::market_id AS market_id,
												   sel_lsp_backgroundcheck::cross_market_key AS cross_market_key,
												   sel_lsp_backgroundcheck::market_key AS market_key
												   ;


table_tf_dim_service_provider = 
    FOREACH sel_lsp_ecommercestatus 
    GENERATE
(legacy_spid IS NULL?-1:legacy_spid) AS legacy_spid,
(new_world_spid IS NULL?-1:new_world_spid) AS new_world_spid,
(al4_company_name IS NULL?legacy_company_name:al4_company_name) AS company_nm,
(legacy_company_name IS NULL?'':legacy_company_name) AS service_provider_group_type,
(al4_joined_date IS NULL?leagcy_entered_date:al4_joined_date) AS entered_dt,
(al4_city IS NULL?legacy_city:al4_city) AS city,
(al4_abbreviation IS NULL?legacy_state:al4_abbreviation) AS state,
(al4_postal_code IS NULL?legacy_zip:al4_postal_code) AS postal_code,
((legacy_exclude_reason IS NOT NULL AND legacy_exclude_reason !='')?1:0) AS is_excluded,
(WebAdvertiser IS NULL?0:WebAdvertiser) AS web_advertiser,
(CallCenterAdvertiser IS NULL?0:CallCenterAdvertiser) AS call_center_advertiser,
(PubAdvertiser IS NULL?0:PubAdvertiser) AS pub_advertiser,
(IsInsured IS NULL?0:IsInsured) AS is_insured,
(IsBonded IS NULL?0:IsBonded) AS is_bonded,
(IsLicensed IS NULL?0:IsLicensed) AS is_licensed,
(BackgroundCheck IS NULL?0:BackgroundCheck) AS background_check,
(ecommercestatus IS NULL?0:ecommercestatus) AS ecommerce_status,
(vintage==3?'Health':(vintage==2?'P1':(vintage==1?'P2+':NULL))) AS vintage,
(market_id IS NULL?cross_market_key:market_key) AS market_key,
ToDate('$EST_TIME','yyyy-MM-dd HH:mm:ss') as est_load_timestamp,
ToDate('$UTC_TIME','yyyy-MM-dd HH:mm:ss') as utc_load_timestamp;

/* Loading to target */

STORE table_tf_dim_service_provider into 'work_shared_dim.tf_dim_service_provider'
    using org.apache.hive.hcatalog.pig.HCatStorer();


