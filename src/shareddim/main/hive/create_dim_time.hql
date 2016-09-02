--/*
--  AUTHOR       : Abhijeet Purwar
--  DATE         : JUN 28, 2016
--  DESCRIPTION  : Creates a external Dim_Time table in Gold layer.
--                 It does not have any source.
--                 It is one time dimension table load using java jar utility Dim_Time.jar.
--*/

--/*  CREATING A DATEDIM DIMENSION TABLE IN GOLD LAYER.
--*/


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dim_time
(
time_key INT,
time_ak STRING,
hour_24 STRING,
hour_12 STRING,
minute STRING,
half_hour STRING,
am_pm STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hivevar:S3_BUCKET}/data/gold/shareddim/dim_time'
tblproperties("skip.header.line.count"="1");