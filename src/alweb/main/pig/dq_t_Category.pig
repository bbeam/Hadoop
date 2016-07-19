/*
PIG SCRIPT    : dq_t_category.pig
AUTHOR        : Ashoka Reddy
DATE          : JUN 24, 2016
DESCRIPTION   : Data quallity check and cleansing for source table t_Category
*/

/* LOADING THE LOOKUP TABLES */
table_t_category = LOAD '$ALWEB_INCOMING_DB.t_category' USING org.apache.hive.hcatalog.pig.HCatLoader();

/* DATA CLEANSING ; CHECK TAB_CATEGORY FOR NULL CATEGORYID */
SPLIT table_category INTO 
	table_t_category_good IF categoryid IS NOT NULL,
	table_t_category_bad IF categoryid IS NULL;
	

/* ADDING ERROR_CODE AND ERROR_DESC FOR THE BAD RECORDS */

table_t_category_bad_error_table = FOREACH table_t_category_bad
				  GENERATE *,10001 AS error_code:INT, 'null CategoryId is not allowed' AS error_desc:CHARARRAY,$BUSDATE AS bus_date:CHARARRAY;


/* STORING THE BAD RECORDS INTO ERROR TABLE  */
STORE table_t_category_bad_error_table INTO '$ALWEB_GOLD_DB.err_dq_t_category' USING org.apache.hive.hcatalog.pig.HCatStorer();


/* Generate table t_Sku with datatype according to the source schema */
table_t_category_good_table = FOREACH table_t_category_good
								GENERATE  (INT)categoryid AS categoryid:INT,
											(CHARARRAY)name AS name:CHARARRAY,
											(CHARARRAY)status AS status:CHARARRAY,
											ToDate(createdate,'yyyy/MM/dd HH:mm:ss') AS createdate:DATETIME,
											(CHARARRAY)createby AS createby:CHARARRAY,
											ToDate(updatedate,'yyyy/MM/dd HH:mm:ss') AS updatedate:DATETIME,
											(CHARARRAY)updateby AS updateby:CHARARRAY,
											(INT)error_code AS error_code:INT,
											(CHARARRAY)error_desc AS error_desc:CHARARRAY

/* STORING THE GOOD RECORDS IN DQ TABLE */
STORE table_t_category_good_table INTO '$WORK_DB.dq_t_category' USING org.apache.hive.hcatalog.pig.HCatStorer();