/*
PIG SCRIPT    : DQ_t_Sku.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quality check and cleansing for source table t_Sku.
*/


/* Register PiggyBank jar and define function for UDF such as isNumeric, etc*/

register file:/usr/lib/pig/piggybank.jar

DEFINE IsInt org.apache.pig.piggybank.evaluation.IsInt;


/* LOADING THE LOOKUP TABLES */
table_t_sku = 
	LOAD '$ALWEB_INCOMING_DB.$TABLE_INC_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();

filter_table_t_sku = FILTER table_t_sku BY loaddate=='$LOADDATE';


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	filter_table_t_sku 
	INTO 
	table_t_sku_SkuId_bad IF skuid IS NULL OR skuid == '',
	table_t_sku_SkuId_isInt_bad IF skuid IS NOT NULL AND skuid != '' AND NOT(IsInt(skuid)),
	table_t_sku_AlId_isInt_bad IF alid IS NOT NULL AND  alid != '' AND NOT(IsInt(alid)),
	table_t_sku_ContractId_bad IF contractid IS NULL OR  contractid == '',
	table_t_sku_ContractId_isInt_bad IF contractid IS NOT NULL AND  contractid != '' AND NOT(IsInt(contractid)),
	table_t_sku_Title_bad IF title IS NULL OR  title == '',
	table_t_sku_Description_bad IF description IS NULL OR  description == '',
	table_t_sku_TandC_bad IF termsandconditions IS NULL OR  termsandconditions == '',
	table_t_sku_Status_bad IF status IS NULL OR  status == '',
	table_t_sku_SkuType_bad IF skutype IS NULL OR  skutype == '',
	table_t_sku_StartDateTime_bad IF startdatetime IS NULL OR  startdatetime == '',
	table_t_sku_EndDateTime_bad IF enddatetime IS NULL OR  enddatetime == '',
	table_t_sku_MinQuantity_bad IF minquantity IS NULL OR  minquantity == '',
	table_t_sku_MinQuantity_isInt_bad IF minquantity IS NOT NULL AND minquantity != '' AND NOT(IsInt(minquantity)),
	table_t_sku_MaxQuantity_bad IF maxquantity IS NULL OR  maxquantity == '',
	table_t_sku_MaxQuantity_isInt_bad IF maxquantity IS NOT NULL AND  maxquantity != '' AND NOT(IsInt(maxquantity)),
	table_t_sku_MaxPurchaseQuantity_bad IF maxpurchasequantity IS NULL OR  maxpurchasequantity == '',
	table_t_sku_MaxPurchaseQuantity_isInt_bad IF maxpurchasequantity IS NOT NULL AND  maxpurchasequantity != '' AND NOT(IsInt(maxpurchasequantity)),
	table_t_sku_RapidConnect_bad IF rapidconnect IS NULL OR  rapidconnect == '',
	table_t_sku_RapidConnect_isInt_bad IF rapidconnect IS NOT NULL AND  rapidconnect != '' AND NOT(IsInt(rapidconnect)),
	table_t_sku_IsAutoRenew_bad IF isautorenew IS NULL OR  isautorenew == '',
	table_t_sku_IsAutoRenew_isInt_bad IF isautorenew IS NOT NULL AND  isautorenew != '' AND NOT(IsInt(isautorenew)),
	table_t_sku_ProductId_isInt_bad IF productid IS NOT NULL AND  productid != '' AND NOT(IsInt(productid)),
	table_t_sku_Version_bad IF version IS NULL OR  version == '',
	table_t_sku_Version_isInt_bad IF version IS NOT NULL AND  version != '' AND NOT(IsInt(version)),
	table_t_sku_IsEmailPromotable_bad IF isemailpromotable IS NULL OR  isemailpromotable == '',
	table_t_sku_IsEmailPromotable_isInt_bad IF isemailpromotable IS NOT NULL AND  isemailpromotable != '' AND NOT(IsInt(isemailpromotable)),
	table_t_sku_CreateDate_bad IF createdate IS NULL OR  createdate == '',
	table_t_sku_CreateBy_bad IF createby IS NULL OR  createby == '',
	table_t_sku_CreateBy_isInt_bad IF createby IS NOT NULL AND  createby != '' AND NOT(IsInt(createby)),
	table_t_sku_UpdateDate_bad IF updatedate IS NULL OR updatedate == '',
	table_t_sku_UpdateBy_bad IF updateby IS NULL OR  updateby == '',
	table_t_sku_UpdateBy_isInt_bad IF updateby IS NOT NULL AND  updateby != '' AND NOT(IsInt(updateby)),
	table_t_sku OTHERWISE;


/* Adding additional fields, error_type and error_desc */
table_t_sku_SkuId = 
	FOREACH table_t_sku_SkuId_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty SkuId is not allowed' AS error_desc:CHARARRAY;

table_t_sku_ContractId = 
	FOREACH table_t_sku_ContractId_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty ContractId is not allowed' AS error_desc:CHARARRAY;

table_t_sku_Title = 
	FOREACH table_t_sku_Title_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty Title is not allowed' AS error_desc:CHARARRAY;

table_t_sku_Description = 
	FOREACH table_t_sku_Description_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty Description is not allowed' AS error_desc:CHARARRAY;

table_t_sku_TandC = 
	FOREACH table_t_sku_TandC_bad
GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty TermsAndConditions is not allowed' AS error_desc:CHARARRAY;

table_t_sku_Status = 
	FOREACH table_t_sku_Status_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty Status is not allowed' AS error_desc:CHARARRAY;
	
table_t_sku_SkuType = 
	FOREACH table_t_sku_SkuType_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty SkuType is not allowed' AS error_desc:CHARARRAY;

table_t_sku_StartDateTime = 
	FOREACH table_t_sku_StartDateTime_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty StartDateTime is not allowed' AS error_desc:CHARARRAY;

table_t_sku_EndDateTime = 
	FOREACH table_t_sku_EndDateTime_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty EndDateTime is not allowed' AS error_desc:CHARARRAY;

table_t_sku_MinQuantity = 
	FOREACH table_t_sku_MinQuantity_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty MinQuantity is not allowed' AS error_desc:CHARARRAY;

table_t_sku_MaxQuantity = 
	FOREACH table_t_sku_MaxQuantity_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty MaxQuantity is not allowed' AS error_desc:CHARARRAY;

table_t_sku_MaxPurchaseQuantity = 
	FOREACH table_t_sku_MaxPurchaseQuantity_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty MaxPurchaseQuantity is not allowed' AS error_desc:CHARARRAY;
	
table_t_sku_RapidConnect = 
	FOREACH table_t_sku_RapidConnect_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty RapidConnect is not allowed' AS error_desc:CHARARRAY;

table_t_sku_IsAutoRenew = 
	FOREACH table_t_sku_IsAutoRenew_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty IsAutoRenew is not allowed' AS error_desc:CHARARRAY;

table_t_sku_Version = 
	FOREACH table_t_sku_Version_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty Version is not allowed' AS error_desc:CHARARRAY;
	
table_t_sku_IsEmailPromotable = 
	FOREACH table_t_sku_IsEmailPromotable_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty IsEmailPromotable is not allowed' AS error_desc:CHARARRAY;

table_t_sku_CreateDate = 
	FOREACH table_t_sku_CreateDate_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty CreateDate is not allowed' AS error_desc:CHARARRAY;
		
table_t_sku_CreateBy = 
	FOREACH table_t_sku_CreateBy_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty CreateBy is not allowed' AS error_desc:CHARARRAY;

table_t_sku_UpdateDate = 
	FOREACH table_t_sku_UpdateDate_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty UpdateDate is not allowed' AS error_desc:CHARARRAY;

table_t_sku_UpdateBy = 
	FOREACH table_t_sku_UpdateBy_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty UpdateBy is not allowed' AS error_desc:CHARARRAY;
	
table_t_sku_SkuId_isInt = 
	FOREACH table_t_sku_SkuId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'SkuId is not an Integer' AS error_desc:CHARARRAY;
	
table_t_sku_AlId_isInt = 
	FOREACH table_t_sku_AlId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'AlId is not an Integer' AS error_desc:CHARARRAY;
	
table_t_sku_ContractId_isInt = 
	FOREACH table_t_sku_ContractId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'ContractId is not an Integer' AS error_desc:CHARARRAY;
	
table_t_sku_MinQuantity_isInt = 
	FOREACH table_t_sku_MinQuantity_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'MinQuantity is not an Integer' AS error_desc:CHARARRAY;
	
table_t_sku_MaxQuantity_isInt = 
	FOREACH table_t_sku_MaxQuantity_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'MaxQuantity is not an Integer' AS error_desc:CHARARRAY;
	
table_t_sku_MaxPurchaseQuantity_isInt = 
	FOREACH table_t_sku_MaxPurchaseQuantity_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'MaxQuantity is not an Integer' AS error_desc:CHARARRAY;
	
table_t_sku_RapidConnect_isInt = 
	FOREACH table_t_sku_RapidConnect_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'RapidConnect is not an Integer' AS error_desc:CHARARRAY;
		
table_t_sku_IsAutoRenew_isInt = 
	FOREACH table_t_sku_IsAutoRenew_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'IsAutoRenew is not an Integer' AS error_desc:CHARARRAY;
			
table_t_sku_ProductId_isInt = 
	FOREACH table_t_sku_ProductId_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'ProductId is not an Integer' AS error_desc:CHARARRAY;
				
table_t_sku_Version_isInt = 
	FOREACH table_t_sku_Version_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'Version is not an Integer' AS error_desc:CHARARRAY;
				
table_t_sku_IsEmailPromotable_isInt = 
	FOREACH table_t_sku_IsEmailPromotable_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'IsEmailPromotable is not an Integer' AS error_desc:CHARARRAY;
					
table_t_sku_CreateBy_isInt = 
	FOREACH table_t_sku_CreateBy_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'CreateBy is not an Integer' AS error_desc:CHARARRAY;
						
table_t_sku_UpdateBy_isInt = 
	FOREACH table_t_sku_UpdateBy_isInt_bad 
	GENERATE *, '$NAN_CHECK_TYPE' AS error_type:CHARARRAY, 'UpdateBy is not an Integer' AS error_desc:CHARARRAY;


/* Duplicate check for skuid */
count_t_sku_skuId = FOREACH (GROUP filter_table_t_sku BY skuid) 
					GENERATE filter_table_t_sku, COUNT(filter_table_t_sku.skuid) AS count:LONG;
					
duplicate_skuId = FOREACH (FILTER count_t_sku_skuId BY count > 1) 
				 GENERATE FLATTEN(filter_table_t_sku), '$DUPLICATE_CHECK_TYPE' AS error_type:CHARARRAY, 
				 									   'Duplicate SkuId is not allowed' AS error_desc:CHARARRAY;

table_t_sku = FOREACH (FILTER count_t_sku_skuId by count == 1) 
			  GENERATE FLATTEN(filter_table_t_sku);


/* JOINING ALL THE BAD RECORDS */
table_t_sku_bad_join = UNION table_t_sku_SkuId ,table_t_sku_ContractId ,table_t_sku_Title ,table_t_sku_Description ,
					   table_t_sku_TandC ,table_t_sku_Status ,table_t_sku_SkuType ,table_t_sku_StartDateTime ,
					   table_t_sku_EndDateTime ,table_t_sku_MinQuantity ,table_t_sku_MaxQuantity ,table_t_sku_MaxPurchaseQuantity ,
					   table_t_sku_RapidConnect ,table_t_sku_IsAutoRenew ,table_t_sku_Version ,table_t_sku_IsEmailPromotable ,
					   table_t_sku_CreateDate ,table_t_sku_CreateBy ,table_t_sku_UpdateDate ,table_t_sku_UpdateBy ,
					   table_t_sku_SkuId_isInt ,table_t_sku_AlId_isInt ,table_t_sku_ContractId_isInt ,table_t_sku_MinQuantity_isInt ,
					   table_t_sku_MaxQuantity_isInt ,table_t_sku_MaxPurchaseQuantity_isInt ,table_t_sku_RapidConnect_isInt ,
					   table_t_sku_IsAutoRenew_isInt ,table_t_sku_ProductId_isInt ,table_t_sku_Version_isInt ,table_t_sku_IsEmailPromotable_isInt ,
					   table_t_sku_CreateBy_isInt ,table_t_sku_UpdateBy_isInt ,duplicate_skuId;

table_t_sku_bad = FOREACH table_t_sku_bad_join
				  GENERATE skuid ,alid ,contractid ,title ,description ,termsandconditions ,status ,skutype ,
				  startdatetime ,enddatetime ,minquantity ,maxquantity ,maxpurchasequantity ,rapidconnect ,
				  isautorenew ,productid ,version ,placement ,isemailpromotable ,createdate ,createby ,
				  updatedate ,updateby ,error_type ,error_desc ,ToString(CurrentTime()) AS dqtimestamp:CHARARRAY;

/* Generate table good records of t_Sku with datatype according to the source schema */
table_t_sku = FOREACH table_t_sku
				   GENERATE (INT)skuid AS skuid:INT ,(INT)alid AS alid:INT ,(INT)contractid AS contractid:INT ,
				   (CHARARRAY)title AS title:CHARARRAY ,(CHARARRAY)description AS description:CHARARRAY ,
				   (CHARARRAY)termsandconditions AS termsandconditions:CHARARRAY ,(CHARARRAY) status AS status:CHARARRAY ,
				   (CHARARRAY)skutype AS skutype:CHARARRAY ,ToDate(startdatetime,'yyyy-MM-dd HH:mm:ss.SSS') AS startdatetime:DATETIME ,
				   ToDate(enddatetime,'yyyy-MM-dd HH:mm:ss.SSS') AS enddatetime:DATETIME ,(INT)minquantity AS minquantity:INT ,
				   (INT)maxquantity AS maxquantity:INT ,(INT)maxpurchasequantity AS maxpurchasequantity:INT ,
				   (INT)rapidconnect AS rapidconnect:INT ,(INT)isautorenew AS isautorenew:INT ,(INT)productid AS productid:INT ,
				   (INT)version AS version:INT ,(CHARARRAY)placement AS placement:CHARARRAY ,
				   (INT)isemailpromotable AS isemailpromotable:INT ,ToDate(createdate,'yyyy-MM-dd HH:mm:ss.SSS') AS createdate:DATETIME ,
				   (INT)createby AS createby:INT ,ToDate(updatedate,'yyyy-MM-dd HH:mm:ss.SSS') AS updatedate :DATETIME ,
				   (INT)updateby AS updateby:INT;

/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE table_t_sku_bad 
	INTO '$S3_LOCATION_OPERATIONS_DATA/$SOURCE_ALWEB/$SOURCE_SCHEMA/$TABLE_ERR_DQ_T_SKU/loaddate=$LOADDATE'
	USING PigStorage('\u0001');
	
	
STORE table_t_sku 
	INTO '$WORK_DB.$TABLE_DQ_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();