%declare PREV_DATE `date -d '1 day ago' +%Y-%m-%d`;
%declare CURR_DATE `date +%Y-%m-%d`;

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
filter_table_dq_contract_item = FILTER table_dq_contract_item BY start_date <= ToDate($PREV_DATE) and end_date > ToDate($PREV_DATE) ;
join_contract_wt_contract_item = JOIN table_dq_contract BY contract_id LEFT OUTER,filter_table_dq_contract_item BY contract_id;
join_vintage_wt_category= JOIN join_contract_wt_contract_item BY category_id LEFT OUTER,table_dq_categories BY category_id;
join_vintage_wt_category_group= JOIN join_vintage_wt_category BY category_group_id LEFT OUTER,table_dq_category_group BY category_group_id;
join_vintage_wt_category_group_type= JOIN join_vintage_wt_category_group BY category_group_type_id LEFT OUTER,table_dq_category_group_type BY category_group_type_id;
join_vintage_wt_ad_element= JOIN join_vintage_wt_category_group_type BY ad_element_id LEFT OUTER,table_dq_ad_element BY ad_element_id;

vintage_null_check = FOREACH join_vintage_wt_ad_element generate table_dq_contract::sp_id as sp_id,
(join_vintage_wt_category_group_type::join_vintage_wt_category_group::table_dq_category_group::category_group_type_id is NULL?0:join_vintage_wt_category_group_type::join_vintage_wt_category_group::table_dq_category_group::category_group_type_id) as category_group_type_id,
(table_dq_ad_element::is_advertiser is NULL?0:table_dq_ad_element::is_advertiser) AS is_advertiser,
(join_vintage_wt_category_group_type::join_vintage_wt_category_group::join_vintage_wt_category::join_contract_wt_contract_item::table_dq_contract::publishing_department_id is NULL?0:join_vintage_wt_category_group_type::join_vintage_wt_category_group::join_vintage_wt_category::join_contract_wt_contract_item::table_dq_contract::publishing_department_id) AS publishing_department_id;

sel_pre_vintage= FOREACH vintage_null_check generate sp_id as sp_id, 
((category_group_type_id==2 AND is_advertiser==1)?3:((publishing_department_id==10 AND is_advertiser==1)?2:1)) as (pre_vintage:int);

grp_vintage= GROUP sel_pre_vintage BY sp_id;

sel_vintage= FOREACH grp_vintage GENERATE group as vin_sp_id,MAX(sel_pre_vintage.pre_vintage) AS vintage;

/* Start Process for Advertisers */

join_adelement_wt_category= JOIN join_contract_wt_contract_item BY ad_element_id LEFT OUTER,table_dq_ad_element BY ad_element_id;
join_adtype_wt_adelement= JOIN join_adelement_wt_category BY ad_type_id LEFT OUTER,table_dq_ad_type BY ad_type_id;

filter_table_dq_location = FILTER table_dq_location BY location_id in (1,2,3);

join_location_wt_adtype= JOIN join_adtype_wt_adelement BY location_id LEFT OUTER,filter_table_dq_location BY location_id;

sel_pre_advertiser = FOREACH join_location_wt_adtype GENERATE                   join_adtype_wt_adelement::join_adelement_wt_category::join_contract_wt_contract_item::table_dq_contract::sp_id AS sp_id,
                                                              ((filter_table_dq_location::location_id==1)?1:0) AS WebAdvertiser,
                                                              ((filter_table_dq_location::location_id==2)?1:0) AS CallCenterAdvertiser,
					                                          ((filter_table_dq_location::location_id==3)?1:0) AS PubAdvertiser;

grp_sel_pre_advertiser = GROUP sel_pre_advertiser BY sp_id;

sel_advertiser = FOREACH grp_sel_pre_advertiser GENERATE group as advertiser_sp_id,
        (SUM(sel_pre_advertiser.WebAdvertiser)>=1?1:0) AS WebAdvertiser,
        (SUM(sel_pre_advertiser.CallCenterAdvertiser)>=1?1:0) AS CallCenterAdvertiser,
	    (SUM(sel_pre_advertiser.PubAdvertiser)>=1?1:0) AS PubAdvertiser;

		
/* Start Process for ServiceProviderDocument */
		
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
filter_table_dq_service_provider_background_check = FILTER table_dq_service_provider_background_check BY background_check_status_id ==1;

grp_table_dq_service_provider_background_check = GROUP filter_table_dq_service_provider_background_check BY service_provider_id;

sel_service_provider_background_check = FOREACH grp_table_dq_service_provider_background_check GENERATE group as bg_check_sp_id, 
                                 1 AS BackgroundCheck;
/* Start Process for ECommerceStatus */

filter_table_dq_storefront_order = FILTER table_dq_storefront_order BY create_date>=ToDate($PREV_DATE) AND create_date<ToDate($CURR_DATE);
join_storefront_order_wt_line_item = JOIN filter_table_dq_storefront_order BY storefront_order_id,table_dq_storefront_order_line_item BY storefront_order_id;
join_storefront_line_item_wt_item = JOIN join_storefront_order_wt_line_item BY storefront_item_id, table_dq_storefront_item BY storefront_item_id;
join_excludetestspids_wt_storefront_item= JOIN join_storefront_line_item_wt_item BY sp_id LEFT OUTER,table_dq_exclude_test_sp_ids BY sp_id;

filter_excludetestspids_wt_storefront_item= FILTER join_excludetestspids_wt_storefront_item BY table_dq_exclude_test_sp_ids::sp_id IS NULL;

sel_sp_id1= FOREACH filter_excludetestspids_wt_storefront_item GENERATE join_storefront_line_item_wt_item::table_dq_storefront_item::storefront_item_id as sp_id; 


filter_table_dq_storefront_item_history= FILTER table_dq_storefront_item_history BY start_datetime<ToDate($CURR_DATE) AND end_datetime>=ToDate($PREV_DATE) AND history_date<ToDate($CURR_DATE) AND (history_end_date IS NULL?ToDate('2058-01-01'):history_end_date)>ToDate($PREV_DATE) AND storefront_item_status_id==2;

join_excludetestspids_wt_storefront_item_history= JOIN filter_table_dq_storefront_item_history BY sp_id LEFT OUTER,table_dq_exclude_test_sp_ids by sp_id;

filter_excludetestspids_wt_storefront_item_history= FILTER join_excludetestspids_wt_storefront_item_history BY table_dq_exclude_test_sp_ids::sp_id IS NULL;

sel_sp_id2 = FOREACH filter_excludetestspids_wt_storefront_item_history GENERATE filter_table_dq_storefront_item_history::sp_id AS sp_id;


filter_inn_storefront_item = FILTER table_dq_storefront_item BY start_datetime<ToDate($CURR_DATE) AND end_datetime>=ToDate($PREV_DATE)  AND storefront_item_status_id==2;

join_excludetestspids_inn_wt_inn_storefront_item= JOIN filter_inn_storefront_item BY sp_id LEFT OUTER,table_dq_exclude_test_sp_ids by sp_id;

filter_excludetestspids_inn_wt_inn_storefront_item= FILTER join_excludetestspids_inn_wt_inn_storefront_item BY table_dq_exclude_test_sp_ids::sp_id IS NULL;

sel_sp_id3 = FOREACH filter_excludetestspids_inn_wt_inn_storefront_item GENERATE filter_inn_storefront_item::sp_id AS sp_id;

union_sp_id= UNION sel_sp_id1,sel_sp_id2,sel_sp_id3;

distinct_sp_id= DISTINCT union_sp_id;


join_storefront_item_distinct_sp_id = JOIN table_dq_storefront_item BY sp_id,distinct_sp_id BY sp_id;

sel_pre_ecommercestatus = FOREACH join_storefront_item_distinct_sp_id GENERATE table_dq_storefront_item::sp_id AS ecom_sp_id, 1 AS ecommercestatus;

sel_ecommercestatus = DISTINCT sel_pre_ecommercestatus;

/* Start Process for Al4 SP_ID */

join_t_service_provider_wt_address = JOIN table_dq_t_service_provider BY service_provider_id LEFT OUTER,table_dq_t_service_provider_address BY service_provider_id;

grp_join_t_service_provider_wt_address= GROUP join_t_service_provider_wt_address BY (table_dq_t_service_provider::al_id,table_dq_t_service_provider::service_provider_id,table_dq_t_service_provider::name,table_dq_t_service_provider::joined_date);

sel_pre_al4_attributes = FOREACH grp_join_t_service_provider_wt_address GENERATE FLATTEN(group) AS (al_id,sp_id,company_name,joined_date),MAX(join_t_service_provider_wt_address.table_dq_t_service_provider_address::update_date) as update_date;


join_al4_wt_sp_address = JOIN sel_pre_al4_attributes BY (sp_id , update_date)LEFT OUTER,table_dq_t_service_provider_address BY (service_provider_id , update_date);

join_al4_wt_postal_address = JOIN join_al4_wt_sp_address BY (postal_address_id) LEFT OUTER,table_dq_t_postal_address BY (postal_address_id);

join_al4_wt_city = JOIN join_al4_wt_postal_address BY city_id LEFT OUTER,table_dq_t_city BY city_id;

join_al4_wt_region = JOIN join_al4_wt_city BY table_dq_t_city::region_id LEFT OUTER,table_dq_t_region BY region_id;


sel_al4_attributes = FOREACH join_al4_wt_region GENERATE 
join_al4_wt_city::join_al4_wt_postal_address::join_al4_wt_sp_address::sel_pre_al4_attributes::sp_id AS al4_sp_id,
join_al4_wt_city::join_al4_wt_postal_address::join_al4_wt_sp_address::sel_pre_al4_attributes::al_id AS al_id,
join_al4_wt_city::join_al4_wt_postal_address::join_al4_wt_sp_address::sel_pre_al4_attributes::company_name AS company_name,
join_al4_wt_city::join_al4_wt_postal_address::join_al4_wt_sp_address::sel_pre_al4_attributes::joined_date AS joined_date,
join_al4_wt_city::table_dq_t_city::name AS city,
table_dq_t_region::abbreviation AS abbreviation,
join_al4_wt_city::join_al4_wt_postal_address::table_dq_t_postal_address::postal_code AS postal_code;


/*Start process for market attributes*/

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
join_lsp_wt_market = JOIN table_dq_service_provider BY sp_id LEFT OUTER,sel_market_attributes BY market_sp_id;
join_lsp_wt_sp_grp_asso = JOIN join_lsp_wt_market BY table_dq_service_provider::sp_id LEFT OUTER,table_dq_service_provider_group_association BY sp_id;
join_lsp_wt_sp_grp = JOIN join_lsp_wt_sp_grp_asso BY service_provider_group_id LEFT OUTER,table_dq_service_provider_group BY service_provider_group_id;
join_lsp_wt_sp_grp_type = JOIN join_lsp_wt_sp_grp  BY service_provider_group_type_id LEFT OUTER,table_dq_service_provider_group_type BY service_provider_group_type_id;
join_lsp_wt_alsp= JOIN join_lsp_wt_sp_grp_type BY join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id FULL OUTER,sel_al4_attributes BY al_id;
join_lsp_wt_vintage= JOIN join_lsp_wt_alsp BY join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id LEFT OUTER,sel_vintage BY vin_sp_id;
join_lsp_wt_advertiser= JOIN join_lsp_wt_vintage BY join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id LEFT OUTER,sel_advertiser BY advertiser_sp_id;
join_lsp_wt_doctype= JOIN join_lsp_wt_advertiser BY join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id LEFT OUTER,sel_service_provider_document BY doc_sp_id;
join_lsp_wt_backgroundcheck= JOIN join_lsp_wt_doctype BY join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id LEFT OUTER,sel_service_provider_background_check BY bg_check_sp_id;
join_lsp_wt_ecommercestatus= JOIN join_lsp_wt_backgroundcheck BY join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id LEFT OUTER,sel_ecommercestatus BY ecom_sp_id;

simplified_fileds=	
FOREACH join_lsp_wt_ecommercestatus GENERATE 	
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::sp_id AS legacy_spid,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::sel_al4_attributes::al4_sp_id  AS new_world_spid,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::sel_al4_attributes::company_name AS al4_company_name,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::company_name AS legacy_company_name,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::table_dq_service_provider_group_type::service_provider_group_type_name as service_provider_group_type_name,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::sel_al4_attributes::joined_date AS al4_joined_date,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::entered_date as leagcy_entered_date,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::sel_al4_attributes::city AS al4_city,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::city as legacy_city,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::sel_al4_attributes::abbreviation AS al4_abbreviation,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::state as legay_state,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::sel_al4_attributes::postal_code AS al4_postal_code,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::zip as legacy_zip,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::table_dq_service_provider::exclude_reason AS legacy_exclude_reason,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::sel_advertiser::WebAdvertiser AS WebAdvertiser,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::sel_advertiser::CallCenterAdvertiser AS CallCenterAdvertiser,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::sel_advertiser::PubAdvertiser AS PubAdvertiser,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::sel_service_provider_document::IsInsured AS IsInsured,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::sel_service_provider_document::IsBonded AS IsBonded,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::sel_service_provider_document::IsLicensed  AS IsLicensed,
join_lsp_wt_backgroundcheck::sel_service_provider_background_check::BackgroundCheck as BackgroundCheck,
sel_ecommercestatus::ecommercestatus AS ecommercestatus,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::sel_vintage::vintage AS vintage,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::sel_market_attributes::market_id AS market_id,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::sel_market_attributes::cross_market_key AS cross_market_key,
join_lsp_wt_backgroundcheck::join_lsp_wt_doctype::join_lsp_wt_advertiser::join_lsp_wt_vintage::join_lsp_wt_alsp::join_lsp_wt_sp_grp_type::join_lsp_wt_sp_grp::join_lsp_wt_sp_grp_asso::join_lsp_wt_market::sel_market_attributes::market_key AS market_key;


table_tf_dim_service_provider = 
	FOREACH simplified_fileds 
	GENERATE
legacy_spid AS legacy_spid,
new_world_spid  AS new_world_spid,
(al4_company_name IS NULL?legacy_company_name:al4_company_name) AS company_nm,
(legacy_company_name IS NULL?'':legacy_company_name) AS service_provider_group_type,
(al4_joined_date IS NULL?leagcy_entered_date:al4_joined_date) AS entered_dt,
(al4_city IS NULL?legacy_city:al4_city) AS city,
(al4_abbreviation IS NULL?legay_state:al4_abbreviation) AS state,
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
(market_id IS NULL?cross_market_key:market_key) AS market_key;


rmf user/hadoop/data/work/segment/tf_service_provider/


STORE table_tf_dim_service_provider
                INTO 'user/hadoop/data/work/segment/tf_service_provider/'
                USING PigStorage('\u0001');


