/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_login.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_angieslistweb_prod_user_login.
#########################################################################################################*/

table_dq_member_logon_history = 
        LOAD '$GOLD_LEGACY_ANGIE_DBO_DB.dq_member_logon_history'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_dq_member_logon_history = FILTER table_dq_member_logon_history BY edh_bus_date=='$EDH_BUS_DATE';

table_dq_user_login=
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_user_login'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_user_login= FILTER table_dq_user_login BY edh_bus_date=='$EDH_BUS_DATE';

		
table_dim_event_type=
        LOAD '$GOLD_SHARED_DIM_DB.dim_event_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

jn_table_dq_user_login_wt_member = JOIN table_dq_user_login BY (INT)user_id LEFT , table_dim_member BY user_id;

sel_user_login = FOREACH jn_table_dq_user_login_wt_member GENERATE (INT)table_dim_member::member_id AS member_id:int,
                                                                    (INT)table_dq_user_login::user_id AS user_id:int,
                                                                    est_sent_at AS est_sent_at,
																	(chararray)id as id:chararray;																	

jn_table_dq_member_logon_history_wt_member = JOIN table_dq_member_logon_history BY (INT)member_id LEFT , table_dim_member BY member_id;

sel_member_logon_history = FOREACH jn_table_dq_member_logon_history_wt_member GENERATE 
                                                                    (INT)table_dq_member_logon_history::member_id AS member_id:int,
																	(INT)table_dim_member::user_id AS user_id:int,
                                                                    est_logon_date AS est_sent_at,
																	(chararray)member_logon_history_id AS id:chararray;																
                                         
union_mlh_ul= (UNION sel_user_login,sel_member_logon_history);
		
tf_login = FOREACH union_mlh_ul  GENERATE 
              (CHARARRAY)id AS (id:chararray),
             (INT)(ToString(est_sent_at,'YYYYMMDD')) AS (date_ak:int),
             ToString(est_sent_at,'hh:mm') AS (time_ak:chararray),
             (INT)$NUMERIC_NA_KEY AS (legacy_spid:int),
             (INT)$NUMERIC_NA_KEY AS (new_world_spid:int),
             (INT)$NUMERIC_NA_KEY AS (source_ak:int),
             (chararray)'$STRING_NA_VALUE' AS (source_table:chararray),
             (member_id IS NULL?(INT)$NUMERIC_MISSING_KEY:(INT)member_id) AS (member_id:int),
             (user_id IS NULL?(INT)$NUMERIC_MISSING_KEY:(INT)user_id) AS (user_id:int),
             (INT)$NUMERIC_NA_KEY AS (category_id:int),
             'Login-Web' AS (event_type:chararray),
             (chararray)'$STRING_NA_VALUE' AS (search_type:chararray),
             (INT)0 AS (event_source:int),
             'web' AS (event_sub_source:chararray),
             (chararray)'$STRING_NA_VALUE' AS (search_text:chararray),
            (INT)1 AS (qty:int),
			'1' AS (event_type_key:chararray);

			
join_tf_login_wt_event= JOIN  tf_login BY (event_type,search_type,event_source,event_sub_source),table_dim_event_type  BY (event_type,search_type,event_source,event_sub_source);
			
final_webmetrics_login = 	FOREACH join_tf_login_wt_event GENERATE
                                                 (CHARARRAY)id AS (id:chararray),
                                                 (int)tf_login::date_ak AS (date_ak:int),
                                                 (chararray)tf_login::time_ak AS (time_ak:chararray) ,
                                                 (int)tf_login::legacy_spid AS (legacy_spid:int),
                                                 (int)tf_login::new_world_spid AS (new_world_spid:int),
                                                 (int)tf_login::source_ak  AS (source_ak:int),
                                                 (chararray)tf_login::source_table  AS (source_table:chararray) ,
                                                 (int)tf_login::member_id  AS (member_id:int) ,
                                                 (int)tf_login::user_id AS (user_id:int) ,
                                                 (int)tf_login::category_id AS (category_id:int),
                                                 (chararray)tf_login::event_type AS (event_type:chararray) ,
                                                 (chararray)tf_login::search_type AS (search_type:chararray),
                                                 (int)tf_login::event_source AS (event_source:int) ,
                                                 (chararray)tf_login::event_sub_source AS (event_sub_source:chararray),
                                                 (chararray)tf_login::search_text AS (search_text:chararray),
                                                 (int)tf_login::qty AS (qty:int),
                                                (chararray)table_dim_event_type::event_type_key AS (event_type_key:chararray);

STORE final_webmetrics_login
	INTO 'work_al_web_metrics.tf_nk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();
