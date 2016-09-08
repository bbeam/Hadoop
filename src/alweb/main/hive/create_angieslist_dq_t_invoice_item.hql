--/*
--  HIVE SCRIPT  : create_angieslist_dq_t_invoice_item.hql
--  AUTHOR       : Varun Rauthan
--  DATE         : Sep 07, 2016
--  DESCRIPTION  : Creation of hive incoming table(AngiesList.t_InvoiceItem). 
--*/

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_t_invoice_item
(
invoice_item_id INT,
invoice_id INT,
sku_id INT,
item_type VARCHAR(254),
kind VARCHAR(254),
price DECIMAL,
quantity INT,
description STRING,
prepaid_job_sku_item_id INT,
coupon_code VARCHAR(45),
version INT,
create_date TIMESTAMP,
create_by INT,
update_date TIMESTAMP,
update_by INT
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/alweb/angieslist/full/daily/dq_t_invoice_item';