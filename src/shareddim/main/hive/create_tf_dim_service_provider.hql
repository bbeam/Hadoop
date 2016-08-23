*/--
--  HIVE SCRIPT  : create_tf_dim_service_provider.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_dim_service_provider). 

--*/

--  Creating a DQ hive table(tf_alwp_user_login) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.tf_dim_service_provider
(
legacy_spid int,
new_world_spid  int,
company_nm  string,
service_provider_group_type string,
entered_dt  timestamp,
city    string,
state   string, 
postal_code string,
is_excluded tinyint,
web_advertiser  tinyint,
call_center_advertiser  tinyint,
pub_advertiser  tinyint,
is_insured  tinyint,
is_bonded   tinyint,
is_licensed tinyint,
background_check  tinyint,
ecommerce_status tinyint,
vintage string,
market_key int
 )
LOCATION
  '$HDFS_LOCATION/shareddim/tf_dim_service_provider';
  