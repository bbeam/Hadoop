/*
PIG SCRIPT    : dq_ad_element.pig
AUTHOR        : Anil Aleppy
DATE          : Tue Aug 09 
DESCRIPTION   : Data Transformation script for dim_market dimension
*/



/* Reading the input table */
table_legacy_market=
	LOAD 'gold_legacy_angie_dbo.dq_markets'
	USING org.apache.hive.hcatalog.pig.HCatLoader();

/* Mapping source columns to target */
table_dim_market =
	FOREACH table_legacy_market
	GENERATE market_id  AS market_id:INT, market AS market_nm:CHARARRAY;

/* Loading to target */
STORE table_dim_market into 'work_shared_dim.tf_dim_market'
		using org.apache.hive.hcatalog.pig.HCatStorer();
