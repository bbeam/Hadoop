/*
PIG SCRIPT    : tf_dim_category.pig
AUTHOR        : Anil Aleppy
DATE          : 18 Aug 16 
DESCRIPTION   : Data Transformation script for dim_product dimension
*/

/* Reading the input tables */
legacy_storefront         	=  LOAD 'gold_legacy_angie_dbo.dq_storefront_item'           	USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_storefront_item_type =  LOAD 'gold_legacy_angie_dbo.dq_storefront_item_type'       	USING org.apache.hive.hcatalog.pig.HCatLoader();
al_t_sku 					=  LOAD 'gold_alweb_al.dq_t_sku'  								USING org.apache.hive.hcatalog.pig.HCatLoader();   
al_lead 			  		=  LOAD 'gold_alweb_ffmnt.dq_lead'                   			USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_product         		=  LOAD 'gold_legacy_angie_dbo.dq_product'           			USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_product_type			=  LOAD 'gold_legacy_angie_dbo.dq_product_type'           		USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_ad_element			=  LOAD 'gold_legacy_angie_dbo.dq_ad_element'           		USING org.apache.hive.hcatalog.pig.HCatLoader();   
legacy_ad_type				=  LOAD 'gold_legacy_angie_dbo.dq_ad_type'           			USING org.apache.hive.hcatalog.pig.HCatLoader();   
   	

/*Deriving  CategoryGroup  For each record in angie.dbo.categories by joining with angie.dbo.categorygroup on CategoryGroupId. */
get_category_group = FOREACH (JOIN legacy_categories BY category_group_id LEFT OUTER, legacy_category_group BY category_group_id)
						GENERATE legacy_category_group::category_group_type_id AS category_group_type_id, legacy_categories::category_id AS category_id , 
						legacy_categories::category_name AS category_name, legacy_categories::category AS category, 
						legacy_category_group::category_group AS category_group,  legacy_categories::is_active AS is_active ;
						
						
/*Deriving CategoryGroupType For each record in angie.dbo.categories  by joining with angie.dbo.categorygrouptype on CategoryGroupTypeId.*/ 
get_category_group_type = FOREACH (JOIN get_category_group BY category_group_type_id LEFT OUTER, legacy_category_group_type BY category_group_type_id)
						  GENERATE category_id AS category_id , category_name AS category_name, category AS category, category_group AS category_group, 
						  (BOOLEAN)is_active AS is_active, category_group_type AS category_group_type;
/* Full outer join the dataset with angieslist.dbo.t_category on categoryid to get both legacy and new categories*/
join_alweb_legacy = JOIN al_category BY category_id FULL OUTER , get_category_group_type BY category_id;

/*generate transformation columns 
generate_transformed_column = FOREACH join_alweb_legacy 
								GENERATE 
									(get_category_group_type::category_id IS NOT NULL ?  get_category_group_type::category_id : al_category::category_id ) 	AS category_id , 
									(get_category_group_type::category IS NOT NULL ? get_category_group_type::category : al_category::name) AS category,
									 get_category_group_type::category 																		AS legacy_category,
									 al_category::name 																						AS new_world_category,
									 get_category_group_type::category_name 																AS additional_category_nm,
									 get_category_group_type::is_active 																	AS is_active,
									 get_category_group_type::category_group 																AS category_group,
									 get_category_group_type::category_group_type 															AS category_group_type;
									 
						
STORE generate_transformed_column INTO 'work_shared_dim.tf_dim_category' USING org.apache.hive.hcatalog.pig.HCatStorer();