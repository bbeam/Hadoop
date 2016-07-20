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
					   GENERATE source_ak, 
					   't_Sku' AS source_table:CHARARRAY, 
					   'SkuID' AS source_column:CHARARRAY,
					   'E-Commerce' AS master_product_group:CHARARRAY, 
					   (isemailpromotable == 1 ? 'BigDeal':'Storefront') AS product_type:CHARARRAY, 
					   product,
					   unit_price,
					   'AL4.0' AS source:CHARARRAY
STORE table_br_dim_product 
	INTO '$WORK_DB.br_dim_product' 
	USING org.apache.hive.hcatalog.pig.HCatStorer();