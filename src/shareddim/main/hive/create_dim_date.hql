--
--  AUTHOR       : Abhijeet Purwar
--  DATE         : JUN 28, 2016
--  DESCRIPTION  : Creates a external Dim_Date table in Gold layer.
--                 It does not have any source.
--                 It is one time dimension table load using java jar utility Dim_Date.jar.
--

--  CREATING A DATEDIM DIMENSION TABLE IN GOLD LAYER.
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_date
(
date_key BIGINT,
date_ak DATE,
date_string STRING,
day_number_of_week TINYINT,
week_ending_date DATE,
week_ending_string STRING,
last_day_of_month_date DATE,
last_day_of_month_name STRING,
day_number_of_month TINYINT,
day_number_of_year SMALLINT,
calendar_week TINYINT,
calendar_month TINYINT,
calendar_quarter TINYINT,
calendar_year SMALLINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_date';