/*#########################################################################################################
PIG SCRIPT                              :tf_alwp_user_login.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load tf_angieslistweb_prod_user_login.
#########################################################################################################*/

table_dq_user_login=
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_user_login'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

tf_alwp_user_login = FOREACH table_dq_user_login GENERATE 
             sent_at AS raw_date,
             (INT)(ToString(est_sent_at,'YYYYMMDD')) AS (date_ak:int),
             ToString(est_sent_at,'hh:mm') AS time_ak,
             (INT)(ToString(sent_at,'YYYYMMDD')) AS (date_utc_ak:int),
             ToString(sent_at,'hh:mm') AS time_utc_ak,
             NULL AS (legacy_spid:int),
             NULL AS (nw_spid:int),
             NULL AS (product_id:int),
             NULL AS (product_table:chararray),
             NULL AS (member_id:int),
             (INT)user_id AS (user_id:int),
             NULL AS (category_id:int),
             NULL AS (category_text:chararray),
             'angieslistweb_prod' AS (source_system:chararray),
             'Web'  AS (source_platform:chararray),
             context_user_agent AS user_agent,
             NULL AS (email_campaign_id:int),
             NULL AS (page_url:chararray),
             'Login' AS (event_type:chararray),
             NULL AS (search_type:chararray),
             NULL AS (target_zip:chararray),
             user_zip_code AS source_zip,
             NULL AS (search_text:chararray),
             NULL AS (source_ak_int:int),
             id AS source_ak_text,
             'Segment.angieslistweb_prod.user_login' AS (source_ak_table:chararray),
             NULL AS (anonymous_id:chararray),
             (INT)1 AS (qty:int);

rmf /user/hadoop/data/work/webmetrics/tf_alwp_user_login/			 
			 
STORE tf_alwp_user_login 
	INTO '/user/hadoop/data/work/webmetrics/tf_alwp_user_login/'
	USING PigStorage('\u0001');