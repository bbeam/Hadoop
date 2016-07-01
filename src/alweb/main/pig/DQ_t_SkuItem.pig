/*
PIG SCRIPT    : DQ_t_SkuItem.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quallity check and cleansing for source table t_Sku.
*/

/* LOADING THE LOOKUP TABLES */
table_t_skuitem = 
	LOAD '$ALWEB_INCOMING_DB.$TABLE_INC_T_SKUITEM' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	table_t_skuitem
	INTO 
	table_t_skuitem_SkuItemId_good IF skuitemid IS NOT NULL AND  skuitemid != '',
	table_t_skuitem_SkuItemId_bad IF skuitemid IS NULL OR  skuitemid == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_SkuItemId = 
	FOREACH table_t_skuitem_SkuItemId_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty SkuItemId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_SkuItemId_good
	INTO 
	table_t_skuitem_ContextEntityId_good IF contextentityid IS NOT NULL AND  contextentityid != '',
	table_t_skuitem_ContextEntityId_bad IF contextentityid IS NULL OR  contextentityid == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_ContextEntityId = 
	FOREACH table_t_skuitem_ContextEntityId_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty ContextEntityId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_ContextEntityId_good
	INTO 
	table_t_skuitem_EntityType_good IF entitytype IS NOT NULL AND  entitytype != '',
	table_t_skuitem_EntityType_bad IF entitytype IS NULL OR  entitytype == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_EntityType = 
	FOREACH table_t_skuitem_EntityType_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty EntityType is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_EntityType_good
	INTO 
	table_t_skuitem_SkuId_good IF skuid IS NOT NULL AND  skuid != '',
	table_t_skuitem_SkuId_bad IF skuid IS NULL OR  skuid == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_SkuId = 
	FOREACH table_t_skuitem_SkuId_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty SkuId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_SkuId_good
	INTO 
	table_t_skuitem_OriginalPrice_good IF originalprice IS NOT NULL AND  originalprice != '',
	table_t_skuitem_OriginalPrice_bad IF originalprice IS NULL OR  originalprice == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_OriginalPrice = 
	FOREACH table_t_skuitem_OriginalPrice_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty OriginalPrice is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_OriginalPrice_good
	INTO 
	table_t_skuitem_NonMemberPrice_good IF nonmemberprice IS NOT NULL AND  nonmemberprice != '',
	table_t_skuitem_NonMemberPrice_bad IF nonmemberprice IS NULL OR  nonmemberprice == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_NonMemberPrice = 
	FOREACH table_t_skuitem_NonMemberPrice_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty NonMemberPrice is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_skuitem_NonMemberPrice_good
	INTO 
	table_t_skuitem_MemberPrice_good IF memberprice IS NOT NULL AND  memberprice != '',
	table_t_skuitem_MemberPrice_bad IF memberprice IS NULL OR  memberprice == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_MemberPrice = 
	FOREACH table_t_skuitem_MemberPrice_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty MemberPrice is not allowed' AS error_desc:CHARARRAY;
		
SPLIT 
	table_t_skuitem_NonMemberPrice_good
	INTO 
	table_t_skuitem_Version_good IF version IS NOT NULL AND  version != '',
	table_t_skuitem_Version_bad IF version IS NULL OR  version == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_Version = 
	FOREACH table_t_skuitem_Version_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty Version is not allowed' AS error_desc:CHARARRAY;
			
SPLIT 
	table_t_skuitem_Version_good
	INTO 
	table_t_skuitem_CreateDate_good IF createdate IS NOT NULL AND  createdate != '',
	table_t_skuitem_CreateDate_bad IF createdate IS NULL OR  createdate == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_CreateDate = 
	FOREACH table_t_skuitem_CreateDate_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty CreateDate is not allowed' AS error_desc:CHARARRAY;
				
SPLIT 
	table_t_skuitem_CreateDate_good
	INTO 
	table_t_skuitem_CreateBy_good IF createby IS NOT NULL AND  createby != '',
	table_t_skuitem_CreateBy_bad IF createby IS NULL OR  createby == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_CreateBy = 
	FOREACH table_t_skuitem_CreateBy_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty CreateBy is not allowed' AS error_desc:CHARARRAY;
					
SPLIT 
	table_t_skuitem_CreateBy_good
	INTO 
	table_t_skuitem_UpdateDate_good IF updatedate IS NOT NULL AND  updatedate != '',
	table_t_skuitem_UpdateDate_bad IF updatedate IS NULL OR  updatedate == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_UpdateDate = 
	FOREACH table_t_skuitem_UpdateDate_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty UpdateDate is not allowed' AS error_desc:CHARARRAY;
						
SPLIT 
	table_t_skuitem_UpdateDate_good
	INTO 
	table_t_skuitem_UpdateBy_good IF updateby IS NOT NULL AND  updateby != '',
	table_t_skuitem_UpdateBy_bad IF updateby IS NULL OR  updateby == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_skuitem_UpdateBy = 
	FOREACH table_t_skuitem_UpdateBy_bad 
	GENERATE *, 10002 AS error_code:INT, 'null or empty UpdateBy is not allowed' AS error_desc:CHARARRAY;
	
	
/* JOINING ALL THE BAD RECORDS */
table_t_skuitem_bad_join = UNION table_t_skuitem_SkuItemId, table_t_skuitem_ContextEntityId, 
						   table_t_skuitem_EntityType, table_t_skuitem_SkuId, table_t_skuitem_OriginalPrice, 
						   table_t_skuitem_NonMemberPrice, table_t_skuitem_MemberPrice, table_t_skuitem_Version, 
						   table_t_skuitem_CreateDate, table_t_skuitem_CreateBy, table_t_skuitem_UpdateDate, 
						   table_t_skuitem_UpdateBy;

table_t_skuitem_bad = FOREACH table_t_skuitem_bad_join
				  	  GENERATE skuitemid, contextentityid, entitytype, skuid, originalprice, nonmemberprice, 
				 	  memberprice, version, createdate, createby, updatedate, updateby, error_code, error_desc;
				  
				  

/* Generate table t_Sku with datatype according to the source schema */
table_t_skuitem_good = FOREACH table_t_skuitem_UpdateBy_good
					   GENERATE (INT)skuitemid AS skuitemid:INT, (INT)contextentityid AS contextentityid:INT, 
					   (CHARARRAY)entitytype AS entitytype:CHARARRAY, (INT)skuid AS skuid:INT, 
					   (BIGDECIMAL)originalprice AS originalprice:BIGDECIMAL, (BIGDECIMAL)nonmemberprice AS nonmemberprice:BIGDECIMAL, 
					   (BIGDECIMAL)memberprice AS memberprice:BIGDECIMAL, (INT)version AS version:INT, 
					   ToDate(createdate,'yyyy-MM-dd HH:mm:ss.SSS') AS createdate:DATETIME, (INT)createby AS createby:INT, 
					   ToDate(updatedate,'yyyy-MM-dd HH:mm:ss.SSS') AS updatedate:DATETIME, (INT)updateby AS updateby:INT;


/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE table_t_skuitem_bad 
	INTO '$S3_LOCATION_OPERATIONS_DATA/$SOURCE_ALWEB/$ALWEB_OPERATIONS_DB/$EXTRACTIONTYPE_FULL/$FREQUENCY_DAILY/$TABLE_ERR_DQ_T_SKUITEM/loaddate=$DATE'
	USING PigStorage('\u0001');
	
	
STORE table_t_skuitem_good 
	INTO '$WORK_DB.$TABLE_DQ_T_SKUITEM' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();