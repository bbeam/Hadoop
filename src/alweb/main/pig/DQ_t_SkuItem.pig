/*
PIG SCRIPT    : DQ_t_SkuItem.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quality check and cleansing for source table t_Sku.
*/


/* Register PiggyBank jar and define function for UDF such as isNumeric, etc*/

register file:/usr/lib/pig/piggybank.jar

DEFINE IsInt org.apache.pig.piggybank.evaluation.IsInt;
DEFINE IsDouble org.apache.pig.piggybank.evaluation.IsDouble;


/* LOADING THE LOOKUP TABLES */
table_t_skuitem = 
	LOAD '$ALWEB_INCOMING_DB.$TABLE_INC_T_SKUITEM' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();

filter_table_t_skuitem = FILTER table_t_skuitem BY loaddate=='$LOADDATE';


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	table_t_skuitem
	INTO 
	table_t_skuitem_SkuItemId_bad IF skuitemid IS NULL OR  skuitemid == '',
	table_t_skuitem_SkuItemId_isInt_bad IF skuitemid IS NOT NULL AND  skuitemid != '' AND NOT(IsInt(skuitemid)),
	table_t_skuitem_ContextEntityId_bad IF contextentityid IS NULL OR  contextentityid == '',
	table_t_skuitem_ContextEntityId_isInt_bad IF contextentityid IS NOT NULL AND  contextentityid != '' AND NOT(IsInt(contextentityid)),
	table_t_skuitem_EntityType_bad IF entitytype IS NULL OR  entitytype == '',
	table_t_skuitem_SkuId_bad IF skuid IS NULL OR  skuid == '',
	table_t_skuitem_SkuId_isInt_bad IF skuid IS NOT NULL AND  skuid != '' AND NOT(IsInt(skuid)),
	table_t_skuitem_OriginalPrice_bad IF originalprice IS NULL OR  originalprice == '',
	table_t_skuitem_OriginalPrice_isDouble_bad IF originalprice IS NOT NULL AND  originalprice != '' AND NOT(IsDouble(originalprice)),
	table_t_skuitem_NonMemberPrice_bad IF nonmemberprice IS NULL OR  nonmemberprice == '',
	table_t_skuitem_NonMemberPrice_isDouble_bad IF nonmemberprice IS NOT NULL AND  nonmemberprice != '' AND NOT(IsDouble(nonmemberprice)),
	table_t_skuitem_MemberPrice_bad IF memberprice IS NULL OR  memberprice == '',
	table_t_skuitem_MemberPrice_isDouble_bad IF memberprice IS NOT NULL AND  memberprice != '' AND NOT(IsDouble(memberprice)),
	table_t_skuitem_Version_bad IF version IS NULL OR  version == '',
	table_t_skuitem_Version_isInt_bad IF version IS NOT NULL AND  version != '' AND NOT(IsInt(version)),
	table_t_skuitem_CreateDate_bad IF createdate IS NULL OR  createdate == '',
	table_t_skuitem_CreateBy_bad IF createby IS NULL OR  createby == '',
	table_t_skuitem_CreateBy_isInt_bad IF createby IS NOT NULL AND  createby != '' AND NOT(IsInt(createby)),
	table_t_skuitem_UpdateDate_bad IF updatedate IS NULL OR  updatedate == '',
	table_t_skuitem_UpdateBy_bad IF updateby IS NULL OR  updateby == '',
	table_t_skuitem_UpdateBy_isInt_bad IF updateby IS NOT NULL AND  updateby != '' AND NOT(IsInt(updateby)),
	table_t_skuitem OTHERWISE;


/* Adding additional fields, error_type and error_desc */
table_t_skuitem_SkuItemId = 
	FOREACH table_t_skuitem_SkuItemId_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty SkuItemId is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_ContextEntityId = 
	FOREACH table_t_skuitem_ContextEntityId_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty ContextEntityId is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_EntityType = 
	FOREACH table_t_skuitem_EntityType_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty EntityType is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_SkuId = 
	FOREACH table_t_skuitem_SkuId_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty SkuId is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_OriginalPrice = 
	FOREACH table_t_skuitem_OriginalPrice_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty OriginalPrice is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_NonMemberPrice = 
	FOREACH table_t_skuitem_NonMemberPrice_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty NonMemberPrice is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_MemberPrice = 
	FOREACH table_t_skuitem_MemberPrice_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty MemberPrice is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_Version = 
	FOREACH table_t_skuitem_Version_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty Version is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_CreateDate = 
	FOREACH table_t_skuitem_CreateDate_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty CreateDate is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_CreateBy = 
	FOREACH table_t_skuitem_CreateBy_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty CreateBy is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_UpdateDate = 
	FOREACH table_t_skuitem_UpdateDate_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty UpdateDate is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_UpdateBy = 
	FOREACH table_t_skuitem_UpdateBy_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty UpdateBy is not allowed' AS error_desc:CHARARRAY;
	
table_t_skuitem_SkuItemId_isInt = 
	FOREACH table_t_skuitem_SkuItemId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'SkuItemId is not an Integer' AS error_desc:CHARARRAY;
	
table_t_skuitem_ContextEntityId_isInt = 
	FOREACH table_t_skuitem_ContextEntityId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'ContextEntityId is not an Integer' AS error_desc:CHARARRAY;
	
table_t_skuitem_SkuId_isInt = 
	FOREACH table_t_skuitem_SkuId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'SkuId is not an Integer' AS error_desc:CHARARRAY;
	
table_t_skuitem_OriginalPrice_isDouble = 
	FOREACH table_t_skuitem_OriginalPrice_isDouble_bad 
	GENERATE *, '$NAD_CHECK_TYPE' AS error_type:CHARARRAY, 'OriginalPrice is not an double(/decimal)' AS error_desc:CHARARRAY;
	
table_t_skuitem_NonMemberPrice_isDouble = 
	FOREACH table_t_skuitem_NonMemberPrice_isDouble_bad 
	GENERATE *, '$NAD_CHECK_TYPE' AS error_type:CHARARRAY, 'NonMemberPrice is not an double(/decimal)' AS error_desc:CHARARRAY;
	
table_t_skuitem_MemberPrice_isDouble = 
	FOREACH table_t_skuitem_MemberPrice_isDouble_bad 
	GENERATE *, '$NAD_CHECK_TYPE' AS error_type:CHARARRAY, 'MemberPrice is not an double(/decimal)' AS error_desc:CHARARRAY;
	
table_t_skuitem_Version_isInt = 
	FOREACH table_t_skuitem_Version_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'Version is not an Integer' AS error_desc:CHARARRAY;
	
table_t_skuitem_CreateBy_isInt = 
	FOREACH table_t_skuitem_CreateBy_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'CreateBy is not an Integer' AS error_desc:CHARARRAY;
	
table_t_skuitem_UpdateBy_isInt = 
	FOREACH table_t_skuitem_UpdateBy_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'UpdateBy is not an Integer' AS error_desc:CHARARRAY;

	
/* JOINING ALL THE BAD RECORDS */
table_t_skuitem_bad_join = UNION table_t_skuitem_SkuItemId ,table_t_skuitem_ContextEntityId ,table_t_skuitem_EntityType ,
						   table_t_skuitem_SkuId ,table_t_skuitem_OriginalPrice ,table_t_skuitem_NonMemberPrice ,table_t_skuitem_MemberPrice ,
						   table_t_skuitem_Version ,table_t_skuitem_CreateDate ,table_t_skuitem_CreateBy ,table_t_skuitem_UpdateDate ,
						   table_t_skuitem_UpdateBy ,table_t_skuitem_SkuItemId_isInt ,table_t_skuitem_ContextEntityId_isInt ,table_t_skuitem_SkuId_isInt ,
						   table_t_skuitem_OriginalPrice_isDouble ,table_t_skuitem_NonMemberPrice_isDouble ,table_t_skuitem_MemberPrice_isDouble ,
						   table_t_skuitem_Version_isInt ,table_t_skuitem_CreateBy_isInt ,table_t_skuitem_UpdateBy_isInt;

table_t_skuitem_bad = FOREACH table_t_skuitem_bad_join
				  	  GENERATE skuitemid, contextentityid, entitytype, skuid, originalprice, nonmemberprice, 
				 	  memberprice, version, createdate, createby, updatedate, updateby, error_type, error_desc
				 	  ,ToString(CurrentTime()) AS dqtimestamp:CHARARRAY;
				  
				  

/* Generate table t_Sku with datatype according to the source schema */
table_t_skuitem = FOREACH table_t_skuitem
					   GENERATE (INT)skuitemid AS skuitemid:INT, (INT)contextentityid AS contextentityid:INT, 
					   (CHARARRAY)entitytype AS entitytype:CHARARRAY, (INT)skuid AS skuid:INT, 
					   (BIGDECIMAL)originalprice AS originalprice:BIGDECIMAL, (BIGDECIMAL)nonmemberprice AS nonmemberprice:BIGDECIMAL, 
					   (BIGDECIMAL)memberprice AS memberprice:BIGDECIMAL, (INT)version AS version:INT, 
					   ToDate(createdate,'yyyy-MM-dd HH:mm:ss.SSS') AS createdate:DATETIME, (INT)createby AS createby:INT, 
					   ToDate(updatedate,'yyyy-MM-dd HH:mm:ss.SSS') AS updatedate:DATETIME, (INT)updateby AS updateby:INT;


/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE table_t_skuitem_bad 
	INTO '$S3_LOCATION_OPERATIONS_DATA/$SOURCE_ALWEB/$SOURCE_SCHEMA/$TABLE_ERR_DQ_T_SKUITEM/loaddate=$LOADDATE'
	USING PigStorage('\u0001');
	
	
STORE table_t_skuitem 
	INTO '$WORK_DB.$TABLE_DQ_T_SKUITEM' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();