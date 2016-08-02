--/*
--  HIVE SCRIPT  : create_dq_t_service_provider_address.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 2, 2016
--  DESCRIPTION  : Creation of hive DQ table(AngiesList.t_ServiceProviderAddress). 
--  Execute command:
--
--
-- hive -f $S3_BUCKET/src/$SOURCE_ALWEB/main/hive/create_dq_t_service_provider_address.hql \
-- -hivevar ALWEB_GOLD_DB=$ALWEB_GOLD_DB \ 
-- -hivevar S3_BUCKET=$S3_BUCKET \ 
-- -hivevar SOURCE_ALWEB=$SOURCE_ALWEB
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:ALWEB_GOLD_DB}.dq_t_service_provider_address
(
  service_provider_address_id INT,
  al_id INT,
  postal_address_id INT,
  service_provider_id INT,
  is_primary TINYINT,
  version INT,
  create_date TIMESTAMP,
  create_by INT,
  update_date TIMESTAMP,
  update_by INT,
  load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/${hivevar:SOURCE_ALWEB}/angie/full/daily/dq_t_service_provider_address';






`ServiceProviderAddressId` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `AlId` INT(11) NULL DEFAULT NULL,
  `PostalAddressId` INT(11) UNSIGNED NOT NULL,
  `ServiceProviderId` INT(11) UNSIGNED NOT NULL,
  `IsPrimary` BIT(1) NOT NULL,
  `Version` INT(11) UNSIGNED NOT NULL DEFAULT '1',
  `CreateDate` DATETIME(6) NOT NULL,
  `CreateBy` INT(11) UNSIGNED NOT NULL,
  `UpdateDate` DATETIME(6) NOT NULL,
  `UpdateBy` INT(11) UNSIGNED NOT NULL,
  PRIMARY KEY (`ServiceProviderAddressId`),
  INDEX `ix_ServiceProviderAddress_ServiceProviderId` (`ServiceProviderId` ASC),
  INDEX `ix_ServiceProviderAddress_PostalAddressId` (`PostalAddressId` ASC))
ENGINE = InnoDB
AUTO_INCREMENT = 12641226;