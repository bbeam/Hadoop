/*
PIG SCRIPT    : DQ_t_Sku.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quality check and cleansing for source table t_Sku.
*/

/* LOADING THE LOOKUP TABLES */
table_t_sku = 
	LOAD '$ALWEB_INCOMING_DB.$TABLE_INC_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	table_t_sku 
	INTO 
	table_t_sku_SkuId_good IF skuid IS NOT NULL AND skuid != '',
	table_t_sku_SkuId_bad IF skuid IS NULL OR skuid == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_SkuId = 
	FOREACH table_t_sku_SkuId_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty SkuId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_SkuId_good 
	INTO 
	table_t_sku_ContractId_good IF contractid IS NOT NULL AND  contractid != '',
	table_t_sku_ContractId_bad IF contractid IS NULL OR  contractid == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_ContractId = 
	FOREACH table_t_sku_ContractId_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty ContractId is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_ContractId_good 
	INTO 
	table_t_sku_Title_good IF title IS NOT NULL AND  title != '',
	table_t_sku_Title_bad IF title IS NULL OR  title == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Title = 
	FOREACH table_t_sku_Title_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty Title is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_Title_good 
	INTO 
	table_t_sku_Description_good IF description IS NOT NULL AND  description != '',
	table_t_sku_Description_bad IF description IS NULL OR  description == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Description = 
	FOREACH table_t_sku_Description_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty Description is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_Description_good 
	INTO 
	table_t_sku_TandC_good IF termsandconditions IS NOT NULL AND  termsandconditions != '',
	table_t_sku_TandC_bad IF termsandconditions IS NULL OR  termsandconditions == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_TandC = 
	FOREACH table_t_sku_TandC_bad
GENERATE *, 10001 AS error_code:INT, 'null or empty TermsAndConditions is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_TandC_good 
	INTO 
	table_t_sku_Status_good IF status IS NOT NULL AND  status != '',
	table_t_sku_Status_bad IF status IS NULL OR  status == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_Status = 
	FOREACH table_t_sku_Status_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty Status is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_Status_good 
	INTO 
	table_t_sku_StartDateTime_good IF startdatetime IS NOT NULL AND  startdatetime != '',
	table_t_sku_StartDateTime_bad IF startdatetime IS NULL OR  startdatetime == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_StartDateTime = 
	FOREACH table_t_sku_StartDateTime_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty StartDateTime is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_StartDateTime_good 
	INTO 
	table_t_sku_EndDateTime_good IF enddatetime IS NOT NULL AND  enddatetime != '',
	table_t_sku_EndDateTime_bad IF enddatetime IS NULL OR  enddatetime == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_EndDateTime = 
	FOREACH table_t_sku_EndDateTime_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty EndDateTime is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_EndDateTime_good 
	INTO 
	table_t_sku_MinQuantity_good IF minquantity IS NOT NULL AND  minquantity != '',
	table_t_sku_MinQuantity_bad IF minquantity IS NULL OR  minquantity == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_MinQuantity = 
	FOREACH table_t_sku_MinQuantity_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty MinQuantity is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_MinQuantity_good 
	INTO 
	table_t_sku_MaxQuantity_good IF maxquantity IS NOT NULL AND  maxquantity != '',
	table_t_sku_MaxQuantity_bad IF maxquantity IS NULL OR  maxquantity == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_MaxQuantity = 
	FOREACH table_t_sku_MaxQuantity_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty MaxQuantity is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_MaxQuantity_good 
	INTO 
	table_t_sku_RapidConnect_good IF rapidconnect IS NOT NULL AND  rapidconnect != '',
	table_t_sku_RapidConnect_bad IF rapidconnect IS NULL OR  rapidconnect == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_RapidConnect = 
	FOREACH table_t_sku_RapidConnect_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty RapidConnect is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_RapidConnect_good 
	INTO 
	table_t_sku_CreateDate_good IF createdate IS NOT NULL AND  createdate != '',
	table_t_sku_CreateDate_bad IF createdate IS NULL OR  createdate == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_CreateDate = 
	FOREACH table_t_sku_CreateDate_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty CreateDate is not allowed' AS error_desc:CHARARRAY;
		
SPLIT 
	table_t_sku_CreateDate_good 
	INTO 
	table_t_sku_CreateBy_good IF createby IS NOT NULL AND  createby != '',
	table_t_sku_CreateBy_bad IF createby IS NULL OR  createby == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_CreateBy = 
	FOREACH table_t_sku_CreateBy_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty CreateBy is not allowed' AS error_desc:CHARARRAY;
	
SPLIT 
	table_t_sku_CreateBy_good 
	INTO 
	table_t_sku_UpdateDate_good IF updatedate IS NOT NULL AND  updatedate != '',
	table_t_sku_UpdateDate_bad IF updatedate IS NULL OR updatedate == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_UpdateDate = 
	FOREACH table_t_sku_UpdateDate_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty UpdateDate is not allowed' AS error_desc:CHARARRAY;

SPLIT 
	table_t_sku_UpdateDate_good 
	INTO 
	table_t_sku_UpdateBy_good IF updateby IS NOT NULL AND  updateby != '',
	table_t_sku_UpdateBy_bad IF updateby IS NULL OR  updateby == '';

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */
table_t_sku_UpdateBy = 
	FOREACH table_t_sku_UpdateBy_bad 
	GENERATE *, 10001 AS error_code:INT, 'null or empty UpdateBy is not allowed' AS error_desc:CHARARRAY;


/* JOINING ALL THE BAD RECORDS */
table_t_sku_bad_join = UNION table_t_sku_SkuId, table_t_sku_ContractId, table_t_sku_Title, table_t_sku_Description,
				       table_t_sku_TandC, table_t_sku_Status, table_t_sku_StartDateTime, table_t_sku_EndDateTime, 
				       table_t_sku_MinQuantity, table_t_sku_MaxQuantity, table_t_sku_RapidConnect, table_t_sku_CreateDate, 
				       table_t_sku_CreateBy, table_t_sku_UpdateDate, table_t_sku_UpdateBy;

table_t_sku_bad = FOREACH table_t_sku_bad_join
				  GENERATE skuid ,alid ,contractid ,title ,description ,termsandconditions ,status ,skutype ,
				  startdatetime ,enddatetime ,minquantity ,maxquantity ,maxpurchasequantity ,rapidconnect ,
				  isautorenew ,productid ,version ,placement ,isemailpromotable ,createdate ,createby ,
				  updatedate ,updateby ,error_code ,error_desc;

/* Generate table t_Sku with datatype according to the source schema */
table_t_sku_good = FOREACH table_t_sku_UpdateBy_good
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
	INTO '$S3_LOCATION_OPERATIONS_DATA/$SOURCE_ALWEB/$ALWEB_OPERATIONS_DB/$TABLE_ERR_DQ_T_SKU/loaddate=$DATE'
	USING PigStorage('\u0001');
	
	
STORE table_t_sku_good 
	INTO '$WORK_DB.$TABLE_DQ_T_SKU' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();