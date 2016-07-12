--/*
--  AUTHOR       : Abhijeet Purwar
--  DATE         : JUN 28, 2016
--  DESCRIPTION  : Creates a external Dim_Time table in Gold layer.
--                 It does not have any source.
--                 It is one time dimension table load using java jar utility Dim_Time.jar.
--*/

--/*  CREATING A DATEDIM DIMENSION TABLE IN GOLD LAYER.
--*/

CREATE DATABASE IF NOT EXISTS ${hivevar:DIM_TIME_DATABASE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DIM_TIME_DATABASE_NAME}.${hivevar:DIM_TIME_TABLE_NAME}
(
TimeKey INT,
TimeAK STRING,
Hour24 STRING,
Hour12 STRING,
Minute STRING,
HalfHour STRING,
AM_PM STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hivevar:DIM_TIME_HIVE_TABLE_LOCATION_S3}'
tblproperties("skip.header.line.count"="1");