/*#########################################################################################################
PIG SCRIPT                              :sk_fact_web_metrics.pig
AUTHOR                                  :Abhinav Mehar.
DATE                                    :Mon Aug 22 07:53:37 UTC 2016
DESCRIPTION                             :Pig script to create load sk_fact_web_metrics.
#########################################################################################################*/

table_tf_fact_web_metrics=
        LOAD '$WORK_AL_WEBMETRICS_DB.tf_fact_web_metrics'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_market=
        LOAD '$GOLD_SHARED_DIM_DB.dim_market'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_product=
        LOAD '$GOLD_SHARED_DIM_DB.dim_product'
        USING org.apache.hive.hcatalog.pig.HCatLoader();		
		
table_dim_event_type=
        LOAD '$GOLD_SHARED_DIM_DB.dim_event_type'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_category=
        LOAD '$GOLD_SHARED_DIM_DB.dim_category'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_dim_member=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
table_dim_service_provider=
        LOAD '$GOLD_SHARED_DIM_DB.dim_service_provider'
        USING org.apache.hive.hcatalog.pig.HCatLoader();
		
jn_tf_fwb_wt_prod = JOIN table_tf_fact_web_metrics BY (source_ak,source_table) LEFT,table_dim_product BY (source_ak,source_table);
jn_tf_fwb_wt_event = JOIN jn_tf_fwb_wt_prod BY (event_type,search_type,event_source,event_sub_source) LEFT,table_dim_event_type BY (event_type,search_type,event_source,event_sub_source);
jn_tf_fwb_wt_ctgy =	JOIN jn_tf_fwb_wt_event BY (category_id) LEFT,table_dim_category BY (category_id);
jn_tf_fwb_wt_mbr = JOIN jn_tf_fwb_wt_ctgy BY (member_id,user_id) LEFT,table_dim_member BY (member_id,user_id);
jn_tf_fwb_wt_sp =  JOIN jn_tf_fwb_wt_mbr BY (legacy_spid,new_world_spid) LEFT,table_dim_service_provider BY (legacy_spid,new_world_spid);



STORE final_webmetrics_user_login 
	INTO '$WORK_AL_WEBMETRICS_DB.sk_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();