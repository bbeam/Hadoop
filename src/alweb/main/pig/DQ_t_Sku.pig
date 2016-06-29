/*
PIG SCRIPT    : DQ_t_Sku.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quallity check and cleansing for source table t_Sku.
*/

/* LOADING THE LOOKUP TABLES */
table_t_sku = 
	LOAD 'ALWEB_INCOMING_DB.$TABLE_INC_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	table_t_sku 
	INTO 
	table_t_sku_SkuId_good IF SkuId IS NOT NULL,
	table_t_sku_SkuId_bad IF SkuId IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_SkuId = 
	FOREACH table_t_sku_SkuId_bad 
	GENERATE *, 10001 AS error_code:INT, 'null SkuId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_SkuId_good 
	INTO 
	table_t_sku_ContractId_good IF ContractId IS NOT NULL,
	table_t_sku_ContractId_bad IF ContractId IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_ContractId = 
	FOREACH table_t_sku_ContractId_bad 
	GENERATE *, 10001 AS error_code:INT, 'null ContractId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_ContractId_good 
	INTO 
	table_t_sku_Title_good IF Title IS NOT NULL,
	table_t_sku_Title_bad IF Title IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Title = 
	FOREACH table_t_sku_Title_bad 
	GENERATE *, 10001 AS error_code:INT, 'null Title is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_Title_good 
	INTO 
	table_t_sku_Description_good IF Description IS NOT NULL,
	table_t_sku_Description_bad IF Description IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Description = 
	FOREACH table_t_sku_Description_bad 
	GENERATE *, 10001 AS error_code:INT, 'null Description is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_Description_good 
	INTO 
	table_t_sku_TandC_good IF TermsAndConditions IS NOT NULL,
	table_t_sku_TandC_bad IF TermsAndConditions IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_TandC = 
	FOREACH table_t_sku_TandC_bad
GENERATE *, 10001 AS error_code:INT, 'null TermsAndConditions is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_TandC_good 
	INTO 
	table_t_sku_Status_good IF Status IS NOT NULL,
	table_t_sku_Status_bad IF Status IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Status = 
	FOREACH table_t_sku_Status_bad 
	GENERATE *, 10001 AS error_code:INT, 'null Status is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_Status_good 
	INTO 
	table_t_sku_StartDateTime_good IF StartDateTime IS NOT NULL,
	table_t_sku_StartDateTime_bad IF StartDateTime IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_StartDateTime = 
	FOREACH table_t_sku_StartDateTime_bad 
	GENERATE *, 10001 AS error_code:INT, 'null StartDateTime is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_StartDateTime_good 
	INTO 
	table_t_sku_EndDateTime_good IF EndDateTime IS NOT NULL,
	table_t_sku_EndDateTime_bad IF EndDateTime IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_EndDateTime = 
	FOREACH table_t_sku_EndDateTime_bad 
	GENERATE *, 10001 AS error_code:INT, 'null EndDateTime is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_EndDateTime_good 
	INTO 
	table_t_sku_MinQuantity_good IF MinQuantity IS NOT NULL,
	table_t_sku_MinQuantity_bad IF MinQuantity IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_MinQuantity = 
	FOREACH table_t_sku_MinQuantity_bad 
	GENERATE *, 10001 AS error_code:INT, 'null MinQuantity is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_MinQuantity_good 
	INTO 
	table_t_sku_MaxQuantity_good IF MaxQuantity IS NOT NULL,
	table_t_sku_MaxQuantity_bad IF MaxQuantity IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_MaxQuantity = 
	FOREACH table_t_sku_MaxQuantity_bad 
	GENERATE *, 10001 AS error_code:INT, 'null MaxQuantity is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_MaxQuantity_good 
	INTO 
	table_t_sku_RapidConnect_good IF RapidConnect IS NOT NULL,
	table_t_sku_RapidConnect_bad IF RapidConnect IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_RapidConnect = 
	FOREACH table_t_sku_RapidConnect_bad 
	GENERATE *, 10001 AS error_code:INT, 'null RapidConnect is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_RapidConnect_good 
	INTO 
	table_t_sku_CreateDate_good IF CreateDate IS NOT NULL,
	table_t_sku_CreateDate_bad IF CreateDate IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_CreateDate = 
	FOREACH table_t_sku_CreateDate_bad 
	GENERATE *, 10001 AS error_code:INT, 'null CreateDate is not allowed' AS error_desc:CHARARRAY;
		
SPLIT 
	table_t_sku_CreateDate_good 
	INTO 
	table_t_sku_CreateBy_good IF CreateBy IS NOT NULL,
	table_t_sku_CreateBy_bad IF CreateBy IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_CreateBy = 
	FOREACH table_t_sku_CreateBy_bad 
	GENERATE *, 10001 AS error_code:INT, 'null CreateBy is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_CreateBy_good 
	INTO 
	table_t_sku_UpdateDate_good IF UpdateDate IS NOT NULL,
	table_t_sku_UpdateDate_bad IF UpdateDate IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_UpdateDate = 
	FOREACH table_t_sku_UpdateDate_bad 
	GENERATE *, 10001 AS error_code:INT, 'null UpdateDate is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_UpdateDate_good 
	INTO 
	table_t_sku_UpdateBy_good IF UpdateBy IS NOT NULL,
	table_t_sku_UpdateBy_bad IF UpdateBy IS NULL;

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_UpdateBy = 
	FOREACH table_t_sku_UpdateBy_bad 
	GENERATE *, 10001 AS error_code:INT, 'null UpdateBy is not allowed' AS error_desc:CHARARRAY;


/* JOINING ALL THE BAD RECORDS */
table_t_sku_bad_join = UNION table_t_sku_SkuId, table_t_sku_ContractId, table_t_sku_Title, table_t_sku_Description,
				    table_t_sku_TandC, table_t_sku_Status, table_t_sku_StartDateTime, table_t_sku_EndDateTime, 
				    table_t_sku_MinQuantity, table_t_sku_MaxQuantity, table_t_sku_RapidConnect, table_t_sku_CreateDate, 
				    table_t_sku_CreateBy, table_t_sku_UpdateDate, table_t_sku_UpdateBy;

table_t_sku_bad = FOREACH table_t_sku_bad_join
				  GENERATE *, $DATE AS LoadDate:CHARARRAY;

/* Generate table t_Sku with datatype according to the source schema */
table_t_sku_good = FOREACH table_t_sku_UpdateBy_good
				   GENERATE (INT)skuid AS skuid:INT ,(INT)alid AS alid:INT ,(INT)contractid AS contractid:INT ,
				   (CHARARRAY)title AS title:CHARARRAY ,(CHARARRAY)description AS description:CHARARRAY ,
				   (CHARARRAY)termsandconditions AS termsandconditions:CHARARRAY ,(CHARARRAY) status AS status:CHARARRAY ,
				   (CHARARRAY)skutype AS skutype:CHARARRAY ,ToDate(startdatetime,'yyyy/MM/dd HH:mm:ss') AS startdatetime:DATETIME ,
				   ToDate(enddatetime,'yyyy/MM/dd HH:mm:ss') AS enddatetime:DATETIME ,(INT)minquantity AS minquantity:INT ,
				   (INT)maxquantity AS maxquantity:INT ,(INT)maxpurchasequantity AS maxpurchasequantity:INT ,
				   (INT)rapidconnect AS rapidconnect:INT ,(INT)isautorenew AS isautorenew:INT ,(INT)productid AS productid:INT ,
				   (INT)version AS version:INT ,(CHARARRAY)placement AS placement:CHARARRAY ,
				   (INT)isemailpromotable AS isemailpromotable:INT ,ToDate(createdate,'yyyy/MM/dd HH:mm:ss') AS createdate:DATETIME ,
				   (INT)createby AS createby:INT ,ToDate(updatedate,'yyyy/MM/dd HH:mm:ss') AS updatedate :DATETIME ,
				   (INT)updateby AS updateby:INT;

/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE table_t_sku_bad 
	INTO '$ALWEB_GOLD_DB.$TABLE_ERR_DQ_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();
	
STORE table_t_sku_good 
	INTO '$WORK_DB.$TABLE_DQ_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();