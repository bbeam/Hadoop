/*
PIG SCRIPT    : DQ_t_SkuItem.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quallity check and cleansing for source table t_Sku.
*/

/* LOADING THE LOOKUP TABLES */
table_t_skuitem = 
	LOAD 'ALWEB_INCOMING_DB.$TABLE_INC_T_SKUITEM' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	table_t_skuitem
	INTO 
	table_t_skuitem_SkuItemId_good IF SkuItemId IS NOT NULL,
	table_t_skuitem_SkuItemId_bad IF SkuItemId IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_SkuItemId = 
	FOREACH table_t_skuitem_SkuItemId_bad 
	GENERATE *, 10002 AS error_code:INT, 'null SkuItemId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_SkuItemId_good
	INTO 
	table_t_skuitem_ContextEntityId_good IF ContextEntityId IS NOT NULL,
	table_t_skuitem_ContextEntityId_bad IF ContextEntityId IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_ContextEntityId = 
	FOREACH table_t_skuitem_ContextEntityId_bad 
	GENERATE *, 10002 AS error_code:INT, 'null ContextEntityId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_ContextEntityId_good
	INTO 
	table_t_skuitem_EntityType_good IF EntityType IS NOT NULL,
	table_t_skuitem_EntityType_bad IF EntityType IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_EntityType = 
	FOREACH table_t_skuitem_EntityType_bad 
	GENERATE *, 10002 AS error_code:INT, 'null EntityType is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_EntityType_good
	INTO 
	table_t_skuitem_SkuId_good IF SkuId IS NOT NULL,
	table_t_skuitem_SkuId_bad IF SkuId IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_SkuId = 
	FOREACH table_t_skuitem_SkuId_bad 
	GENERATE *, 10002 AS error_code:INT, 'null SkuId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_SkuId_good
	INTO 
	table_t_skuitem_OriginalPrice_good IF OriginalPrice IS NOT NULL,
	table_t_skuitem_OriginalPrice_bad IF OriginalPrice IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_OriginalPrice = 
	FOREACH table_t_skuitem_OriginalPrice_bad 
	GENERATE *, 10002 AS error_code:INT, 'null OriginalPrice is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_skuitem_OriginalPrice_good
	INTO 
	table_t_skuitem_NonMemberPrice_good IF NonMemberPrice IS NOT NULL,
	table_t_skuitem_NonMemberPrice_bad IF NonMemberPrice IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_NonMemberPrice = 
	FOREACH table_t_skuitem_NonMemberPrice_bad 
	GENERATE *, 10002 AS error_code:INT, 'null NonMemberPrice is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_skuitem_NonMemberPrice_good
	INTO 
	table_t_skuitem_MemberPrice_good IF MemberPrice IS NOT NULL,
	table_t_skuitem_MemberPrice_bad IF MemberPrice IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_MemberPrice = 
	FOREACH table_t_skuitem_MemberPrice_bad 
	GENERATE *, 10002 AS error_code:INT, 'null MemberPrice is not allowed' AS error_desc:CHARARRAY;
		
SPLIT 
	table_t_skuitem_NonMemberPrice_good
	INTO 
	table_t_skuitem_Version_good IF Version IS NOT NULL,
	table_t_skuitem_Version_bad IF Version IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Version = 
	FOREACH table_t_skuitem_Version_bad 
	GENERATE *, 10002 AS error_code:INT, 'null Version is not allowed' AS error_desc:CHARARRAY;
			
SPLIT 
	table_t_skuitem_Version_good
	INTO 
	table_t_skuitem_CreateDate_good IF CreateDate IS NOT NULL,
	table_t_skuitem_CreateDate_bad IF CreateDate IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_CreateDate = 
	FOREACH table_t_skuitem_CreateDate_bad 
	GENERATE *, 10002 AS error_code:INT, 'null CreateDate is not allowed' AS error_desc:CHARARRAY;
				
SPLIT 
	table_t_skuitem_CreateDate_good
	INTO 
	table_t_skuitem_CreateBy_good IF CreateBy IS NOT NULL,
	table_t_skuitem_CreateBy_bad IF CreateBy IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_CreateBy = 
	FOREACH table_t_skuitem_CreateBy_bad 
	GENERATE *, 10002 AS error_code:INT, 'null CreateBy is not allowed' AS error_desc:CHARARRAY;
					
SPLIT 
	table_t_skuitem_CreateBy_good
	INTO 
	table_t_skuitem_UpdateDate_good IF UpdateDate IS NOT NULL,
	table_t_skuitem_UpdateDate_bad IF UpdateDate IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_UpdateDate = 
	FOREACH table_t_skuitem_UpdateDate_bad 
	GENERATE *, 10002 AS error_code:INT, 'null UpdateDate is not allowed' AS error_desc:CHARARRAY;
						
SPLIT 
	table_t_skuitem_UpdateDate_good
	INTO 
	table_t_skuitem_UpdateBy_good IF UpdateBy IS NOT NULL,
	table_t_skuitem_UpdateBy_bad IF UpdateBy IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_UpdateBy = 
	FOREACH table_t_skuitem_UpdateBy_bad 
	GENERATE *, 10002 AS error_code:INT, 'null UpdateBy is not allowed' AS error_desc:CHARARRAY;
	
	
/* JOINING ALL THE BAD RECORDS */
table_t_skuitem_bad_join = UNION table_t_skuitem_SkuItemId_bad, table_t_skuitem_ContextEntityId_bad, 
						   table_t_skuitem_EntityType_bad, table_t_skuitem_SkuId_bad, table_t_skuitem_OriginalPrice_bad, 
						   table_t_skuitem_NonMemberPrice_bad, table_t_skuitem_MemberPrice_bad, table_t_skuitem_Version_bad, 
						   table_t_skuitem_CreateDate_bad, table_t_skuitem_CreateBy_bad, table_t_skuitem_UpdateDate_bad, 
						   table_t_skuitem_UpdateBy_bad;

table_t_skuitem_bad = FOREACH table_t_skuitem_bad_join
				  GENERATE *, $DATE AS LoadDate:CHARARRAY;
				  

/* Generate table t_Sku with datatype according to the source schema */
table_t_skuitem_good = FOREACH table_t_skuitem_UpdateBy_good
					   GENERATE (INT)SkuItemId AS SkuItemId:INT, (INT)ContextEntityId AS ContextEntityId:INT, 
					   (CHARARRAY)EntityType AS EntityType:CHARARRAY, (INT)SkuId AS SkuId:INT, 
					   (DOUBLE)OriginalPrice AS OriginalPrice:DOUBLE, (DOUBLE)NonMemberPrice AS NonMemberPrice:DOUBLE, 
					   (DOUBLE)MemberPrice AS MemberPrice:DOUBLE, (INT)Version AS Version:INT, 
					   ToDate(CreateDate,'yyyy/MM/dd HH:mm:ss') AS CreateDate:DATETIME, (INT)CreateBy AS CreateBy:INT, 
					   ToDate(UpdateDate,'yyyy/MM/dd HH:mm:ss') AS UpdateDate:DATETIME, (INT)UpdateBy AS UpdateBy:INT;


/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE table_t_skuitem_bad 
	INTO '$ALWEB_GOLD_DB.$TABLE_ERR_DQ_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();
	
STORE table_t_skuitem_good 
	INTO '$WORK_DB.$TABLE_DQ_T_SKUITEM' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();