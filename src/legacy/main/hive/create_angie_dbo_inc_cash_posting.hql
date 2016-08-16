--/*
--  HIVE SCRIPT  : create_angie_dbo_inc_cash_posting.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_cash_posting). 
--*/


--  Creating a incoming hive table(inc_cash_posting) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.inc_cash_posting
(
	cash_posting_id STRING,
	invoice_detail_id STRING,
	amount STRING,
	pay_type_id STRING,
	process_date STRING,
	payment_profile_id STRING,
	manually_entered_date STRING,
	employee_id STRING,
	payment_transaction_request_id STRING

)
PARTITIONED BY(edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/legacy/angie/dbo/incremental/daily/inc_cash_posting';