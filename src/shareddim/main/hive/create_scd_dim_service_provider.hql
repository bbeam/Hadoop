--  HIVE SCRIPT  : create_scd_dim_service_provider.hql
--  AUTHOR       : Abhijeet Purwar
--  DATE         : Aug 18, 2016
--  DESCRIPTION  : Creation of hive SCD work table
--
-- hive -f $CREATE_SCD_HQL_PATH \
-- -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
-- -hivevar WORK_DIM_TABLE_NAME=$WORK_DIM_TABLE_NAME

DROP TABLE IF EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:WORK_DIM_DB_NAME}.${hivevar:WORK_DIM_TABLE_NAME}
(
service_provider_key bigint,
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
action_cd STRING,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
 )
LOCATION
  '${hivevar:WORK_DIR}/data/work/shareddim/scd_dim_service_provider';
 