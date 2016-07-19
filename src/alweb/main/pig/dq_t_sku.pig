/*
PIG SCRIPT    : dq_t_sku.pig
AUTHOR        : Varun Rauthan
DATE          : JUN 24, 2016
DESCRIPTION   : Data quality check and cleansing for source table t_Sku.
*/


/* Register PiggyBank jar and define function for UDF such as isNumeric, etc */

register file:/usr/lib/pig/piggybank.jar

DEFINE IsInt org.apache.pig.piggybank.evaluation.IsInt;


/* LOADING THE LOOKUP TABLES */
table_t_sku = 
	LOAD '$ALWEB_INCOMING_DB.inc_t_sku' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();

filter_table_t_sku = FILTER table_t_sku BY bus_date=='$BUSDATE';


/* DATA CLEANSING ,SET DEFAULT FOR NULL */
SPLIT
	filter_table_t_sku
	INTO
	setdefault_t_sku IF isemailpromotable IS NULL OR  isemailpromotable == '',
	dqfilter_table_t_sku_isemailpromotable OTHERWISE;


/* Adding default value in case of null */
table_t_sku_setdefault = 
	FOREACH setdefault_t_sku 
	GENERATE skuid ,alid ,contractid ,title ,description ,termsandconditions ,status ,skutype ,startdatetime ,enddatetime ,minquantity ,maxquantity ,
			 maxpurchasequantity ,rapidconnect ,isautorenew ,productid ,version ,placement ,'0' ,createdate ,createby ,updatedate ,updateby;


filter_table_t_sku = UNION dqfilter_table_t_sku_isemailpromotable, table_t_sku_setdefault;


/* DATA QUALITY CHECK FOR NOT NULL FILEDS */
SPLIT 
	filter_table_t_sku 
	INTO 
	table_t_sku_nullcheck_bad IF skuid IS NULL OR skuid == '' OR 
		   					     contractid IS NULL OR  contractid == '' OR 
								 title IS NULL OR  title == '' OR 
								 description IS NULL OR  description == '' OR 
							 	 termsandconditions IS NULL OR  termsandconditions == '' OR 
								 status IS NULL OR  status == '' OR 
								 skutype IS NULL OR  skutype == '' OR 
								 startdatetime IS NULL OR  startdatetime == '' OR 
								 enddatetime IS NULL OR  enddatetime == '' OR 
								 minquantity IS NULL OR  minquantity == '' OR 
								 maxquantity IS NULL OR  maxquantity == '' OR 
								 maxpurchasequantity IS NULL OR  maxpurchasequantity == '' OR 
								 rapidconnect IS NULL OR  rapidconnect == '' OR 
								 isautorenew IS NULL OR  isautorenew == '' OR 
								 version IS NULL OR  version == '' OR 
								 createdate IS NULL OR  createdate == '' OR 
								 createby IS NULL OR  createby == '' OR 
								 updatedate IS NULL OR updatedate == '' OR 
								 updateby IS NULL OR  updateby == '',
	table_t_sku_datatypemismatch_bad IF skuid IS NOT NULL AND skuid != '' AND NOT(IsInt(skuid)) AND 
									    alid IS NOT NULL AND  alid != '' AND NOT(IsInt(alid)) AND 
									    contractid IS NOT NULL AND  contractid != '' AND NOT(IsInt(contractid)) AND 
									    minquantity IS NOT NULL AND minquantity != '' AND NOT(IsInt(minquantity)) AND 
									    maxquantity IS NOT NULL AND  maxquantity != '' AND NOT(IsInt(maxquantity)) AND 
									    maxpurchasequantity IS NOT NULL AND  maxpurchasequantity != '' AND NOT(IsInt(maxpurchasequantity)) AND 
									    rapidconnect IS NOT NULL AND  rapidconnect != '' AND NOT(IsInt(rapidconnect)) AND 
									    isautorenew IS NOT NULL AND  isautorenew != '' AND NOT(IsInt(isautorenew)) AND 
									    productid IS NOT NULL AND  productid != '' AND NOT(IsInt(productid)) AND 
			 						    version IS NOT NULL AND  version != '' AND NOT(IsInt(version)) AND 
			 						    isemailpromotable IS NOT NULL AND  isemailpromotable != '' AND NOT(IsInt(isemailpromotable)) AND
									    createby IS NOT NULL AND  createby != '' AND NOT(IsInt(createby)) AND 
									    updateby IS NOT NULL AND  updateby != '' AND NOT(IsInt(updateby)),
	table_t_sku OTHERWISE;


/* Adding additional fields, error_type and error_desc */
table_t_sku_nullcheck = 
	FOREACH table_t_sku_nullcheck_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty skuid, contractid, title, description, termsandconditions, status, skutype, startdatetime, 
															 enddatetime, minquantity, maxquantity, maxpurchasequantity, rapidconnect, isautorenew, version, 
															 createdate, createby, updatedate, updateby is not allowed' AS error_desc:CHARARRAY;


table_t_sku_datatypemismatch = 
	FOREACH table_t_sku_datatypemismatch_bad 
	GENERATE *, '$DATATYPE_MISMATCH' AS error_type:CHARARRAY, 'DataType mismatch found in skuid, alid, contractid, minquantity, maxquantity, maxpurchasequantity, 
															   rapidconnect, isautorenew, productid, version, isemailpromotable, createby, updateby' AS error_desc:CHARARRAY;
	


/* Duplicate check for skuid ,all the duplicate are moved to the error tables */
count_t_sku_skuId = FOREACH (GROUP table_t_sku BY skuid) 
					GENERATE table_t_sku, COUNT(table_t_sku.skuid) AS count:LONG;
					
duplicate_skuId = FOREACH (FILTER count_t_sku_skuId BY count > 1) 
				 GENERATE FLATTEN(table_t_sku), '$DUPLICATE_CHECK_TYPE' AS error_type:CHARARRAY, 
				 									   'Duplicate SkuId is not allowed' AS error_desc:CHARARRAY;

table_t_sku = FOREACH (FILTER count_t_sku_skuId by count == 1) 
			  GENERATE FLATTEN(table_t_sku);


/* JOINING ALL THE BAD RECORDS */
table_t_sku_bad_join = UNION table_t_sku_nullcheck ,table_t_sku_datatypemismatch ,duplicate_skuId;

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
	INTO '$S3_LOCATION_OPERATIONS_DATA/$SOURCE_ALWEB/$SOURCE_SCHEMA/err_dq_t_sku/bus_date=$BUSDATE'
	USING PigStorage('\u0001');
	
	
STORE table_t_sku 
	INTO '$WORK_DB.dq_t_sku' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();