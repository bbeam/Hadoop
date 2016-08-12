--/*
--  HIVE SCRIPT  : create_angie_dbo_dq_cash_posting.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jun 23, 2016
--  DESCRIPTION  : Creation of hive DQ table(dq_cash_posting). 
--*/

--  Creating a dq hive table(dq_cash_posting) over the incoming table
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_cash_posting
(
	cash_posting_id INT,
	invoice_detail_id INT,
	amount DECIMAL(10,2),
	pay_type_id INT,
	est_process_date TIMESTAMP,
	process_date TIMESTAMP,
	payment_profile_id INT,
	est_manually_entered_date TIMESTAMP,
	manually_entered_date TIMESTAMP,
	employee_id INT,
	payment_transaction_request_id INT,
	est_load_timestamp TIMESTAMP,
	utc_load_timestamp TIMESTAMP
)
LOCATION '${hivevar:S3_BUCKET}/data/gold/legacy/angie/dbo/full/daily/dq_cash_posting';
