/*#########################################################################################################
PIG SCRIPT                              :tf_fact_wm_alwp_purchased_cart.pig
AUTHOR                                  :Gaurav Maheshwari
DATE                                    :Thursday Aug 25 2016
DESCRIPTION                             :Pig script to create load tf_fact_wm_angieslistweb_prod_purchased_cart.
#########################################################################################################*/

table_dq_purchased_cart=
        LOAD '$GOLD_SEGMENT_EVENTS_ALWP_DB.dq_purchased_cart'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dim_members=
        LOAD '$GOLD_SHARED_DIM_DB.dim_member'
        USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Filter edh_bus_date to process records only for the passed date */
table_dq_purchased_cart= FILTER table_dq_purchased_cart BY edh_bus_date=='$EDHBUSDATE';


/*Process Started for dq_user_login*/
/* Check if user_id is null as user_id in the applicable column. If user_id is null then populate both member_id and user_id as missing */
sel_purchased_cart =  FOREACH table_dq_purchased_cart GENERATE
                           (CHARARRAY)id AS (id:CHARARRAY),
                           (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : NULL) AS (member_id:INT),
						   (user_id IS NULL OR (CHARARRAY)user_id == ''? (INT)$NUMERIC_MISSING_KEY : (INT)user_id) AS (user_id:INT),
						   est_sent_at AS est_sent_at;

/* Split into 2 separate relations the records with user_id missing and those with member_id available */
SPLIT sel_purchased_cart INTO
                    purchased_cart_user_id_missing IF (user_id == (INT)$NUMERIC_MISSING_KEY),
                    purchased_cart_user_id_available IF (user_id != (INT)$NUMERIC_MISSING_KEY);		
	
	/* Join with dim_member table to get the corresponding membr_id for a given user_id */
jn_purchased_cart_user_id_available_users = FOREACH (JOIN purchased_cart_user_id_available BY user_id LEFT , table_dim_members BY user_id) 
                                   GENERATE 
								           purchased_cart_user_id_available::id AS id,
								           (table_dim_members::member_id IS NULL ? $NUMERIC_UNKOWN_KEY : table_dim_members::member_id) AS member_id,
                                           (table_dim_members::user_id IS NULL ? $NUMERIC_UNKOWN_KEY : table_dim_members::user_id) AS user_id,
										   purchased_cart_user_id_available::est_sent_at AS est_sent_at ;
										   
/* Combine the 2 relations one with missing user_id and the other with member_id available */
un_purchased_cart = UNION jn_purchased_cart_user_id_available_users,purchased_cart_user_id_missing	;										   						
						   		
tf_alwp_purchased_cart = FOREACH un_purchased_cart  GENERATE 
             (CHARARRAY)id AS (id:CHARARRAY),
              ToDate(ToString(est_sent_at,'yyyy-MM-dd'),'yyyy-MM-dd') as (date_ak:datetime),
              ToString(est_sent_at,'HH:mm') AS (time_ak:chararray),
             (INT)$NUMERIC_NA_KEY AS (legacy_spid:INT),
             (INT)$NUMERIC_NA_KEY AS (new_world_spid:INT),
             (INT)$NUMERIC_NA_KEY AS (source_ak:int),
             (CHARARRAY)'$STRING_NA_VALUE' AS (source_table:chararray),
             member_id AS member_id:INT,
             user_id AS user_id:INT,
             (INT)$NUMERIC_NA_KEY AS (category_id:INT),
             '$TF_EVENT_NAME' AS (event_type:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_type:CHARARRAY),
             $EVENT_SOURCE_WEB AS (event_source:INT),
             '$EVENT_SUB_SOURCE_WEB' AS (event_sub_source:CHARARRAY),
             (CHARARRAY)'$STRING_NA_VALUE' AS (search_text:CHARARRAY),
            (INT)1 AS (qty:INT),
			 $TF_EVENT_KEY AS (event_type_key:CHARARRAY);

			
	 
STORE tf_alwp_purchased_cart 
	INTO 'work_al_webmetrics.tf_fact_web_metrics'
	USING org.apache.hive.hcatalog.pig.HCatStorer();