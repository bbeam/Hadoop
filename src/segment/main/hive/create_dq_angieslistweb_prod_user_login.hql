--  HIVE SCRIPT  : create_dq_angieslistweb_prod_user_login.hql
--  AUTHOR       : Abhinav Mehar
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive dq table(Dq_user_login). 

--*/

--  Creating a DQ hive table(Dq_user_login) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:DB_NAME}.dq_user_login
(
id  VARCHAR(254),
est_received_at TIMESTAMP,
received_at TIMESTAMP,
uuid    BIGINT,
context_auth_token  VARCHAR(256),
context_ip  VARCHAR(256),
context_library_name    VARCHAR(256),
context_library_version VARCHAR(256),
context_referer VARCHAR(256),
context_user_agent  VARCHAR(256),
event   VARCHAR(256),
event_text  VARCHAR(256),
est_original_timestamp  TIMESTAMP,
original_timestamp  TIMESTAMP,
sent_at TIMESTAMP,
source  VARCHAR(256),
est_timestamp   TIMESTAMP,
timestamp   TIMESTAMP,
user_id VARCHAR(256),
user_zip_code   VARCHAR(256),
anonymous_id    VARCHAR(256),
context_page_path   VARCHAR(256),
context_page_title  VARCHAR(256),
context_page_url    VARCHAR(256),
est_uuid_ts TIMESTAMP,
uuid_ts TIMESTAMP,
est_load_timestamp TIMESTAMP,
utc_load_timestamp TIMESTAMP
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/gold/segment/events/angieslistweb_prod/incremental/daily/dq_user_login';

