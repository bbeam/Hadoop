*/--
--  HIVE SCRIPT  : create_tf_alwp_user_login.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_alwp_user_login). 

--*/

--  Creating a DQ hive table(tf_alwp_user_login) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.tf_alwp_user_login
(
raw_date  timestamp,
date_ak int,
time_ak string,
date_utc_ak int,
time_utc_ak string,
legacy_spid int,
nw_spid int,
product_id  int,
product_table   string,
member_id   int,
user_id int,
category_id int,
category_text   string,
source_system   string,
source_platform string,
user_agent  string,
email_campaign_id   int,
page_url    string,
event_type  string,
search_type string,
target_zip  string,
source_zip  string,
search_text string,
source_ak_int   int,
source_ak_text  string,
source_ak_table string,
anonymous_id    string,
qty int
)
LOCATION
  '$HDFS_LOCATION/webmetrics/tf_alwp_user_login';
