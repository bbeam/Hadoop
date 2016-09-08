--/*
--  HIVE SCRIPT  : create_angieslist_inc_t_invoice_item.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Sep 07, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_InvoiceItem). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_t_invoice_item
(
invoice_item_id STRING,
invoice_id STRING,
sku_id STRING,
item_type STRING,
kind STRING,
price STRING,
quantity STRING,
description STRING,
prepaid_job_sku_item_id STRING,
coupon_code STRING,
version STRING,
create_date STRING,
create_by STRING,
update_date STRING,
update_by STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/inc_t_invoice_item';