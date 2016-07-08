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
	table_t_skuitem_nullcheck_bad IF skuitemid IS NULL OR  skuitemid == '' OR 
									 contextentityid IS NULL OR  contextentityid == '' OR 
									 entitytype IS NULL OR  entitytype == '' OR 
									 skuid IS NULL OR  skuid == '' OR 
									 originalprice IS NULL OR  originalprice == '' OR 
									 nonmemberprice IS NULL OR  nonmemberprice == '' OR 
									 memberprice IS NULL OR  memberprice == '' OR 
									 version IS NULL OR  version == '' OR 
									 createdate IS NULL OR  createdate == '' OR 
									 createby IS NULL OR  createby == '' OR 
									 updatedate IS NULL OR  updatedate == '' OR 
									 updateby IS NULL OR  updateby == '',
	table_t_skuitem_datatypemismatch_bad IF skuitemid IS NOT NULL AND  skuitemid != '' AND NOT(IsInt(skuitemid)) AND 
											contextentityid IS NOT NULL AND  contextentityid != '' AND NOT(IsInt(contextentityid)) AND 
											skuid IS NOT NULL AND  skuid != '' AND NOT(IsInt(skuid)) AND 
											originalprice IS NOT NULL AND  originalprice != '' AND NOT(IsDouble(originalprice)) AND 
											nonmemberprice IS NOT NULL AND  nonmemberprice != '' AND NOT(IsDouble(nonmemberprice)) AND 
											memberprice IS NOT NULL AND  memberprice != '' AND NOT(IsDouble(memberprice)) AND 
											version IS NOT NULL AND  version != '' AND NOT(IsInt(version)) AND 
											createby IS NOT NULL AND  createby != '' AND NOT(IsInt(createby)) AND 
											updateby IS NOT NULL AND  updateby != '' AND NOT(IsInt(updateby)),
	table_t_skuitem OTHERWISE;


/* Adding additional fields, error_type and error_desc */
table_t_skuitem_nullcheck = 
	FOREACH table_t_skuitem_nullcheck_bad 
	GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null or empty (one or many)skuitemid, contextentityid, entitytype, skuid, originalprice, nonmemberprice, 
															 memberprice, version, createdate, createby, createby, updateby is not allowed' AS error_desc:CHARARRAY;

table_t_skuitem_datatypemismatch = 
	FOREACH table_t_skuitem_datatypemismatch_bad 
	GENERATE *, '$DATATYPE_MISMATCH' AS error_type:CHARARRAY, 'DataType mismatch found in (one or many)skuitemid, contextentityid, skuid, originalprice, nonmemberprice, 
															   memberprice, version, createby, updateby' AS error_desc:CHARARRAY;

/* Duplicate check for skuid ,duplicate records are cleansed and with one of the records(amongst duplicate) added to the good record group*/
user_grp = GROUP table_t_skuitem BY skuid;

table_t_skuitem = FOREACH user_grp {
	      uni_rec = LIMIT table_t_skuitem 1;
    	  GENERATE FLATTEN(uni_rec);
};

/* JOINING ALL THE BAD RECORDS */
table_t_skuitem_bad_join = UNION table_t_skuitem_nullcheck_bad ,table_t_skuitem_datatypemismatch_bad;

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