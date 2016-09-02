--  HIVE SCRIPT  : create_tf_dim_service_provider.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Aug 22, 2016
--  DESCRIPTION  : Creation of hive dq table(tf_dim_service_provider). 


--  Creating a DQ hive table(tf_dim_service_provider) over the incoming data
DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME};
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:TF_TABLE_NAME}
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
market_key bigint,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
LOCATION
 '${hivevar:WORK_DIR}/data/work/shareddim/tf_dim_service_provider';
  