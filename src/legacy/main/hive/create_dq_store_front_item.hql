--/*
--  HIVE SCRIPT  : create_dq_service_provider_group.hql
--  AUTHOR       : Anil Aleppy
--  DATE         : Jul 27, 2016
--  DESCRIPTION  : Creation of hive dq table(angie.ServiceProviderGroup) 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_LEGACY/main/hive/create_dq_service_provider_group.hql \
-- -hivevar LEGACY_GOLD_DB=$LEGACY_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_LEGACY=$SOURCE_LEGACY
--
--
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:LEGACY_GOLD_DB}.dq_service_provider_group
(
	store_front_item_id INT ,
	store_front_item_status_id INT ,
	store_front_item_type_id INT ,
	sp_id INT ,
	title STRING ,
	description STRING ,
	redemption_instructions STRING null,
	start_date_time TIMESTAMP ,
	end_date_time TIMESTAMP ,
	contact_name STRING(101) null,
	contact_phone STRING(50) null,
	contact_email STRING(100) null,
	original_price decimal(10, 2) ,
	member_price decimal(10, 2) ,
	store_front_item_fee decimal(10, 2) null,
	ordered_quantity INT ,
	payment_approved TINYINT ,
	store_front_sales_representative_id INT ,
	last_modified_by INT ,
	create_date TIMESTAMP ,
	create_by STRING(50) ,
	update_date TIMESTAMP null,
	update_by STRING(50) null,
	maximum_quantity INT null,
	maximum_quantity_per_member INT null,
	photo_id INT null,
	auto_renew TINYINT ,
	contract_item_sp_id INT null,
	paid_quantity INT ,
	store_front_item_promo_code_id INT null,
	timezone_id INT ,
	editorial STRING(max) null,
	store_front_item_sku STRING(50) null,
	title_with_place_holders STRING(250) ,
	description_with_place_holders STRING(max) ,
	master_store_front_item_id_for_sku TINYINT ,
	member_only TINYINT ,
	employee_owner_id INT null,
	storefrontorderfulfillmentmethodid INT ,
	donotoverridefulfillmentmethod TINYINT null,
	premiumdeal TINYINT ,
	checksum bigINT null,
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_LEGACY}/angie/full/daily/dq_service_provider_group';
