/*#########################################################################################################
PIG SCRIPT                              :tf_alwp_user_login.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_angieslistweb_prod_user_login.
#########################################################################################################*/

table_dq_member_logon_history = 
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_user_login'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_user_login=
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_user_login'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_user_login= FILTER table_dq_user_login BY edh_bus_date=='$EDHBUSDATE';
		
table_dim_event_type=
        LOAD '$GOLD_SHARED_DIM_DB.dim_event_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_dim_members=
        LOAD '$GOLD_SHARED_DIM_DB.dim_members'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

jn_table_dq_user_login_wt_members = JOIN table_dq_user_login BY (INT)user_id LEFT , table_dim_members BY user_id;
		
tf_alwp_user_login = FOREACH jn_table_dq_user_login_wt_members GENERATE 
             (INT)(ToString(est_sent_at,'YYYYMMDD')) AS (date_ak:int),
             ToString(est_sent_at,'hh:mm') AS (time_ak:chararray),
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