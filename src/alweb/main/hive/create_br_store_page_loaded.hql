--/*
--  HIVE SCRIPT  : Create_BR_Store_Page_Loaded.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 14, 2016
--  DESCRIPTION  : Creation of dim_product table in work db(BR_Store_Page_Loaded). 
--*/

-- Create the database if it doesnot exists.
CREATE DATABASE IF NOT EXISTS ${hivevar:WORK_DB};

--  Creating a incoming hive table(TF_dim_product) over the transformed data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DB}.${hivevar:TABLE_BR_STORE_PAGE_LOADED}
(
    Raw_Date TIMESTAMP,
    Legacy_Sp_Id INT,
    Nw_Sw_Id INT,
    Product_ID INT,
    Product_Table INT,
    Member_ID INT,
    User_ID INT,
    Category_ID INT,
    Email_Campaign_Id INT,
    Search_Text STRING,
    Source_AK_Int INT,
    Source_AK_Text STRING,
    Source_AK_Table STRING,
    Anonymous_Id STRING,
    Qty INT
)
LOCATION '${hivevar:HDFS_LOCATION}/${hivevar:SUBJECT_ALWEBMETRICS}/${hivevar:WORK_DB}/${hivevar:TABLE_BR_STORE_PAGE_LOADED}';