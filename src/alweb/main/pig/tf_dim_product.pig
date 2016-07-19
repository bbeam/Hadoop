/*
PIG SCRIPT    : tf_dim_product.pig
AUTHOR        : Varun Rauthan
DATE          : JUL 01, 2016
DESCRIPTION   : Data Transformation and cleansing stage
*/

/* LOADING THE LOOKUP TABLES */
table_dq_t_sku = 
	LOAD '$WORK_DB.dq_t_sku' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();

table_dq_t_skuitem = 
	LOAD '$WORK_DB.dq_t_skuitem' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();


/* JOINING THE TABLES */
join_dq_t_sku_t_skuitem = JOIN
						  table_dq_t_sku BY skuid,
						  table_dq_t_skuitem BY skuid;

	
/* DATA QUALITY CHECK FOR MANDATORY FILEDS */
/* SPLIT INTO GOOD-BAD RECORDS */
SPLIT 
	 join_dq_t_sku_t_skuitem
	 INTO 
	 join_dq_t_sku_t_skuitem_skuid_good IF table_dq_t_sku::skuid IS NOT NULL,
	 join_dq_t_sku_t_skuitem_skuid_bad IF table_dq_t_sku::skuid IS NULL;

/* ADDING ERROR_TYPE AND ERROR_DESC FOR THE BAD RECORDS */
join_dq_t_sku_t_skuitem_skuid = 
		FOREACH join_dq_t_sku_t_skuitem_skuid_bad 
		GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null skuId is not allowed' AS error_desc:CHARARRAY;
		
/* SPLIT INTO GOOD-BAD RECORDS */
SPLIT 
	 join_dq_t_sku_t_skuitem_skuid_good
	 INTO 
	 join_dq_t_sku_t_skuitem_isemailpromotable_good IF table_dq_t_sku::isemailpromotable IS NOT NULL,
	 join_dq_t_sku_t_skuitem_isemailpromotable_bad IF table_dq_t_sku::isemailpromotable IS NULL;

/* ADDING ERROR_TYPE AND ERROR_DESC FOR THE BAD RECORDS */
join_dq_t_sku_t_skuitem_isemailpromotable = 
		FOREACH join_dq_t_sku_t_skuitem_isemailpromotable_bad 
		GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null isemailpromotable is not allowed' AS error_desc:CHARARRAY;
		
/* SPLIT INTO GOOD-BAD RECORDS */
SPLIT 
	 join_dq_t_sku_t_skuitem_isemailpromotable_good
	 INTO 
	 join_dq_t_sku_t_skuitem_title_good IF table_dq_t_sku::title IS NOT NULL AND table_dq_t_sku::title != '',
	 join_dq_t_sku_t_skuitem_title_bad IF table_dq_t_sku::title IS NULL OR table_dq_t_sku::title == '';

/* ADDING ERROR_TYPE AND ERROR_DESC FOR THE BAD RECORDS */
join_dq_t_sku_t_skuitem_title = 
		FOREACH join_dq_t_sku_t_skuitem_title_bad 
		GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null title is not allowed' AS error_desc:CHARARRAY;
		
/* SPLIT INTO GOOD-BAD RECORDS */
SPLIT 
	 join_dq_t_sku_t_skuitem_title_good
	 INTO 
	 join_dq_t_sku_t_skuitem_memberprice_good IF table_dq_t_skuitem::memberprice IS NOT NULL,
	 join_dq_t_sku_t_skuitem_memberprice_bad IF table_dq_t_skuitem::memberprice IS NULL;

/* ADDING ERROR_TYPE AND ERROR_DESC FOR THE BAD RECORDS */
join_dq_t_sku_t_skuitem_memberprice = 
		FOREACH join_dq_t_sku_t_skuitem_memberprice_bad 
		GENERATE *, '$NULL_CHECK_TYPE' AS error_type:CHARARRAY, 'null memberprice is not allowed' AS error_desc:CHARARRAY;


/* JOINING ALL THE BAD RECORDS */
join_dq_t_sku_t_skuitem_bad = UNION join_dq_t_sku_t_skuitem_skuid, join_dq_t_sku_t_skuitem_isemailpromotable, join_dq_t_sku_t_skuitem_title, join_dq_t_sku_t_skuitem_memberprice;

dq_t_sku_t_skuitem_bad = FOREACH join_dq_t_sku_t_skuitem_bad
						 GENERATE table_dq_t_sku::skuid AS t_sku_skuid:INT, table_dq_t_sku::alid AS t_sku_alid:INT, table_dq_t_sku::contractid AS t_sku_contractid:INT, 
						 table_dq_t_sku::title AS t_sku_title:CHARARRAY, table_dq_t_sku::description AS t_sku_description:CHARARRAY, 
						 table_dq_t_sku::termsandconditions AS t_sku_termsandconditions:CHARARRAY, table_dq_t_sku::status AS t_sku_status:CHARARRAY, 
						 table_dq_t_sku::skutype AS t_sku_skutype:CHARARRAY, table_dq_t_sku::startdatetime AS t_sku_startdatetime:DATETIME, 
						 table_dq_t_sku::enddatetime AS t_sku_enddatetime:DATETIME, table_dq_t_sku::minquantity AS t_sku_minquantity:INT, 
						 table_dq_t_sku::maxquantity AS t_sku_maxquantity:INT, table_dq_t_sku::maxpurchasequantity AS t_sku_maxpurchasequantity:INT, 
						 table_dq_t_sku::rapidconnect AS t_sku_rapidconnect:INT, table_dq_t_sku::isautorenew AS t_sku_isautorenew:INT, 
						 table_dq_t_sku::productid AS t_sku_productid:INT, table_dq_t_sku::version AS t_sku_version:INT, 
						 table_dq_t_sku::placement AS t_sku_placement:CHARARRAY, table_dq_t_sku::isemailpromotable AS t_sku_isemailpromotable:INT, 
						 table_dq_t_sku::createdate AS t_sku_createdate:DATETIME, table_dq_t_sku::createby AS t_sku_createby:INT, 
						 table_dq_t_sku::updatedate AS t_sku_updatedate:DATETIME, table_dq_t_sku::updateby AS t_sku_updateby:INT, 
						 table_dq_t_skuitem::skuitemid AS t_skuitem_skuitemid:INT, table_dq_t_skuitem::contextentityid AS t_skuitem_contextentityid:INT, 
						 table_dq_t_skuitem::entitytype AS t_skuitem_entitytype:CHARARRAY, table_dq_t_skuitem::skuid AS t_skuitem_skuid:INT, 
						 table_dq_t_skuitem::originalprice AS t_skuitem_originalprice:bigdecimal, table_dq_t_skuitem::nonmemberprice AS t_skuitem_nonmemberprice:bigdecimal, 
						 table_dq_t_skuitem::memberprice AS t_skuitem_memberprice:bigdecimal, table_dq_t_skuitem::version AS t_skuitem_version:INT, 
						 table_dq_t_skuitem::createdate AS t_skuitem_createdate:DATETIME, table_dq_t_skuitem::createby AS t_skuitem_createby:INT, 
						 table_dq_t_skuitem::updatedate AS t_skuitem_updatedate:DATETIME, table_dq_t_skuitem::updateby AS t_skuitem_updateby:INT,
						 error_type ,error_desc ,ToString(CurrentTime()) AS tftimestamp:CHARARRAY;				
						  
/* GENERATING THE GOOD DIM PRODUCT TABLE */
gen_dim_product_good = FOREACH join_dq_t_sku_t_skuitem_memberprice_good 
					   GENERATE table_dq_t_sku::skuid AS source_ak:INT, table_dq_t_sku::isemailpromotable AS isemailpromotable:INT, 
					   table_dq_t_sku::title AS product:CHARARRAY, table_dq_t_skuitem::memberprice AS unit_price:BIGDECIMAL;
			   

/* STORING THE DATA IN HIVE PARTITIONED BASED ON THE STATUSCODE */
STORE dq_t_sku_t_skuitem_bad 
	INTO '$S3_LOCATION_OPERATIONS_DATA/$SUBJECT_ALWEBMETRICS/err_tf_dim_product/bus_date=$BUSDATE'
	USING PigStorage('\u0001');
	
	
STORE gen_dim_product_good 
	INTO '$WORK_DB.tf_dim_product' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();