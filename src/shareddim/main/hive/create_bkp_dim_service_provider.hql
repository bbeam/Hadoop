--  HIVE SCRIPT  : create_bkp_dim_service_provider.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Aug 9, 2016
--  DESCRIPTION  : Creation of bkp_dim_service_provider table in operations db   

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.bkp_dim_service_provider
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
    est_load_timestamp TIMESTAMP,
    utc_load_timestamp TIMESTAMP    
)
LOCATION '${hivevar:S3_BUCKET}/data/operations/shareddim/bkp_dim_service_provider';