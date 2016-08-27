/*
PIG SCRIPT    : tf_fact_wm_legacy_profile_view.pig
AUTHOR        : Anil Aleppy
DATE          : 26 Aug 16 
DESCRIPTION   : Data Transformation script for webmetrics fact table for the event Profile view from Legacy Source
*/


requested_companies = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_requested_companies'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

request_type_info = 
        LOAD 'gold_legacy_angie_dbo.dq_tbl_request_type_info'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
dim_members=
        LOAD 'gold_shared_dim.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

		

requested_companies_filtered = FILTER requested_companies BY ToString(requested_companies::est_gave_date,'YYYYMMDD')== '$EDHBUSDATE'
								AND requested_companies::sp_id is not null ;
								
jn_requested_companies_filtered_members = JOIN requested_companies_filtered BY member_id LEFT , dim_members BY member_id;

requested_companies_join_request_type_info = JOIN jn_requested_companies_filtered_members BY requested_companies_filtered::request_info_id , 
											request_type_info BY request_info_id;
											

intermediate_legacy_profile_view = FOREACH requested_companies_join_request_type_info GENERATE
									jn_requested_companies_filtered_members::requested_companies_filtered::est_gave_date AS est_gave_date,
									requested_companies_join_request_type_info::dim_members
											
											
		
tf_legacy_profile_view = FOREACH requested_companies_join_request_type_info GENERATE 
             (INT)(ToString(requested_companies::est_gave_date,'YYYYMMDD')) AS (date_ak:int),
             ToString(requested_companies::est_gave_date,'hh:mm') AS (time_ak:chararray),
             $NUMERIC_NA_KEY AS (legacy_spid:int),
             $NUMERIC_NA_KEY AS (nw_spid:int),
             $NUMERIC_NA_KEY AS (source_ak:int),
             '$STRING_NA_VALUE' AS (source_table:chararray),
             (table_dim_members::member_id IS NULL?(INT)$NUMERIC_MISSING_KEY:(INT)table_dim_members::member_id) AS (member_id:int),
             (table_dq_user_login::user_id IS NULL?(INT)$NUMERIC_MISSING_KEY:(INT)table_dq_user_login::user_id) AS (user_id:int),
             $NUMERIC_NA_KEY AS (category_id:int),
             'Login-Web' AS (event_type:chararray),
             '$STRING_NA_VALUE' AS (search_type:chararray),
             0 AS (event_source:int),
             'web' AS (event_sub_source:chararray),
             (chararray)'$STRING_NA_VALUE' AS (search_text:chararray),
            (INT)1 AS (qty:int);

join_tf_alwp_user_login_wt_event= JOIN  tf_alwp_user_login BY (event_type,search_type,event_source,event_sub_source) LEFT,table_dim_event_type  BY (event_type,search_type,event_source,event_sub_source);
			
final_webmetrics_user_login = 	FOREACH join_tf_alwp_user_login_wt_event GENERATE
                                                 tf_alwp_user_login::date_ak AS date_ak,
                                                 tf_alwp_user_login::time_ak AS time_ak ,
                                                 tf_alwp_user_login::legacy_spid AS legacy_spid,
                                                 tf_alwp_user_login::nw_spid AS new_world_spid,
                                                 tf_alwp_user_login::source_ak  AS source_ak,
                                                 tf_alwp_user_login::source_table  AS source_table ,
                                                 tf_alwp_user_login::member_id  AS member_id ,
                                                 tf_alwp_user_login::user_id AS user_id ,
                                                 tf_alwp_user_login::category_id AS category_id,
                                                 tf_alwp_user_login::event_type AS event_type ,
                                                 tf_alwp_user_login::search_type AS search_type,
                                                 tf_alwp_user_login::event_source AS event_source ,
                                                 tf_alwp_user_login::event_sub_source AS event_sub_source,
                                                 tf_alwp_user_login::search_text AS search_text,
                                                 tf_alwp_user_login::qty AS qty,
                                                (chararray)table_dim_event_type::event_type_key AS event_type_key;
	 
STORE final_webmetrics_user_login 
	INTO 'work_al_webmetrics.tf_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();