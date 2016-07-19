/*
PIG SCRIPT    : br_dim_product.pig
AUTHOR        : Ashoka Reddy
DATE          : JUL 06, 2016
DESCRIPTION   : Pig script for applying Bussiness rules to dimension product
*/

/* LOADING THE LOOKUP TABLES */
table_tf_dim_product = 
	LOAD '$WORK_DB.tf_dim_product' 
	USING org.apache.hive.hcatalog.pig.HCatLoader();

	
/* GENERATING THE Required fields for product dimension */
table_br_dim_product = FOREACH table_tf_dim_product 
					   GENERATE t_sku_skuid AS source_ak:INT, 
					   't_Sku' AS source_table:CHARARRAY, 
					   'SkuID' AS source_column:CHARARRAY,
					   'E-Commerce' AS master_product_group:CHARARRAY, 
					   (t_sku_isemailpromotable == (CHARARRAY)1?'BigDeal':'Storefront') AS product_type:CHARARRAY, 
					   t_sku_title AS product:CHARARRAY,
					   t_skuitem_memberprice AS unit_price:BIGDECIMAL, 
					   'AL4.0' AS source:CHARARRAY, 
					   (BOOLEAN)0 AS joins_flag:BOOLEAN, 
					   (BOOLEAN)0 AS renewals_flag:BOOLEAN;
					   
					   
					   
STORE table_br_dim_product 
	INTO '$WORK_DB.BR_DIM_PRODUCT' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();