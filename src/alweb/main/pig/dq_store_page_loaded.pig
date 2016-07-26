/*
PIG SCRIPT    : DQ_Store_Page_Loaded.pig
AUTHOR        : Abhinav Mehar
DATE          : JUL 17, 2016
DESCRIPTION   : Data quality check and cleansing for source table store_page_loaded.
*/


/* Register PiggyBank jar and define function for UDF such as isNumeric, etc */

register file:/usr/lib/pig/piggybank-0.11.0.jar

DEFINE IsInt org.apache.pig.piggybank.evaluation.IsInt;


/* LOADING THE LOOKUP TABLES */
table_store_page_loaded = 
	LOAD '$ALWEB_INCOMING_DB.$TABLE_INC_STORE_PAGE_LOADED' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();

filter_table_store_page_loaded = FILTER table_store_page_loaded BY loaddate=='$LOADDATE';

/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	filter_table_store_page_loaded 
	INTO 
	table_store_page_loaded_nullcheck_bad IF 
	                             id IS NULL OR id =='' OR
	                             anonymous_id IS NULL OR anonymous_id =='' OR
	                             event_text IS NULL OR event_text =='' OR
	                             sent_at IS NULL OR sent_at =='' OR
	                             service_provider_id IS NULL OR service_provider_id =='' OR
	                             user_id IS NULL OR user_id =='',
	table_store_page_loaded_datatypemismatch_bad IF  
	                             (service_provider_id IS NOT NULL AND TRIM(service_provider_id) !='' AND NOT(IsInt(service_provider_id))) OR
	                             (user_id IS NOT NULL AND TRIM(user_id) !=''  AND NOT(IsInt(user_id))),
	table_store_page_loaded OTHERWISE;





/* Adding additional fields, error_type and error_desc */
table_store_page_loaded_nullcheck = 
	FOREACH table_store_page_loaded_nullcheck_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty id,anonymous_id,event_text,sent_at,service_provider_id,user_id is not allowed' AS error_desc:CHARARRAY;


table_store_page_loaded_datatypemismatch = 
	FOREACH table_store_page_loaded_datatypemismatch_bad 
	GENERATE *, '$DATATYPE_MISMATCH' AS error_type:CHARARRAY, 'DataType mismatch found in service_provider_id,user_id' AS error_desc:CHARARRAY;
	



/* JOINING ALL THE BAD RECORDS */
table_store_page_loaded_bad_join = UNION table_store_page_loaded_nullcheck ,table_store_page_loaded_datatypemismatch ;

table_store_page_loaded_bad = FOREACH table_store_page_loaded_bad_join
				  GENERATE id,anonymous_id,event_text,sent_at,service_provider_id,user_id,error_type ,error_desc ,ToString(CurrentTime()) AS dqtimestamp:CHARARRAY;
				  
/* Generate table good records of t_Sku with datatype according to the source schema */
table_store_page_loaded = FOREACH table_store_page_loaded
				   GENERATE id AS id:CHARARRAY,
                                  anonymous_id AS anonymous_id:CHARARRAY,
                                  event_text AS event_text:CHARARRAY,
                                  ToDate(sent_at,'yyyy-MM-dd HH:mm:ss') AS sent_at:DATETIME,
                                  (INT)service_provider_id AS service_provider_id:INT,
                                  (INT)user_id AS user_id:INT			  
                                  ;

/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE table_store_page_loaded_bad 
	INTO 's3://al-edh-dm/data/operations/source_alweb/$SOURCE_SCHEMA_SEGMENT/$TABLE_ERR_DQ_STORE_PAGE_LOADED/loaddate=$LOADDATE'
	USING PigStorage('\u0001');
	
	
STORE table_store_page_loaded 
	INTO '$WORK_DB.$TABLE_DQ_STORE_PAGE_LOADED' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();