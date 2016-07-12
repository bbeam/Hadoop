--/*
--  AUTHOR       : Abhijeet Purwar
--  DATE         : JUN 28, 2016
--  DESCRIPTION  : Creates a external Dim_Date table in Gold layer.
--                 It does not have any source.
--                 It is one time dimension table load using java jar utility Dim_Date.jar.
--*/

--/*  CREATING A DATEDIM DIMENSION TABLE IN GOLD LAYER.
--*/

CREATE DATABASE IF NOT EXISTS ${hivevar:DIM_DATE_DATABASE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DIM_DATE_DATABASE_NAME}.${hivevar:DIM_DATE_TABLE_NAME}
(
DateKey BIGINT,
DateAK TIMESTAMP,
DateString STRING,
DayNumberOfWeek TINYINT,
WeekEndingDate TIMESTAMP,
WeekEndingString STRING,
LastDayOfMonthDate TIMESTAMP,
LastDayOfMonthName STRING,
DayNumberOfMonth TINYINT,
DayNumberOfYear SMALLINT,
CalendarWeek TINYINT,
CalendarMonth TINYINT,
CalendarQuarter TINYINT,
CalendarYear STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hivevar:DIM_DATE_HIVE_TABLE_LOCATION_S3}'
tblproperties("skip.header.line.count"="1");