--/*
--  HIVE SCRIPT  : create_inc_angieslistweb_prod_review_submitted.hql
--  AUTHOR       : Ashoka Reddy
--  DATE         : Jul 13, 2016
--  DESCRIPTION  : Creation of hive incoming table(inc_angieslistweb_prod_review_submitted). 
--*/

--  Creating a incoming hive table(inc_angieslistweb_prod_review_submitted) over the incoming data
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:SEGMENT_INCOMING_DB}.inc_angieslistweb_prod_review_submitted
(
id STRING,
received_at STRING,
uuid STRING,
`overall_13` STRING,
`availability_14` STRING,
`price_14` STRING,
`office_environment_15` STRING,
`quality_15` STRING,
`availability_16` STRING,
`punctuality_16` STRING,
`office_environment_17` STRING,
`staff_friendliness_17` STRING,
`bedside_manner_18` STRING,
`punctuality_18` STRING,
`communication_19` STRING,
`staff_friendliness_19` STRING,
`overall_1` STRING,
`bedside_manner_20` STRING,
`effectiveness_of_treatment_20` STRING,
`billing_and_administration_21` STRING,
`communication_21` STRING,
`effectiveness_of_treatment_22` STRING,
`billing_and_administration_23` STRING,
`coverage_24` STRING,
`responsiveness_25` STRING,
`customer_service_26` STRING,
`professionalism_27` STRING,
`price_2` STRING,
`quality_3` STRING,
`responsiveness_4` STRING,
`punctuality_5` STRING,
`professionalism_6` STRING,
are_status STRING,
category_ids STRING,
city STRING,
context_auth_token STRING,
context_ip STRING,
context_library_name STRING,
context_library_version STRING,
context_referer STRING,
context_user_agent STRING,
draft_review_id STRING,
event STRING,
event_text STRING,
grades_1 STRING,
grades_13 STRING,
grades_14 STRING,
grades_15 STRING,
grades_16 STRING,
grades_17 STRING,
grades_18 STRING,
grades_19 STRING,
grades_2 STRING,
grades_20 STRING,
grades_21 STRING,
grades_22 STRING,
grades_23 STRING,
grades_24 STRING,
grades_25 STRING,
grades_26 STRING,
grades_27 STRING,
grades_3 STRING,
grades_4 STRING,
grades_5 STRING,
grades_6 STRING,
job_id STRING,
latitude STRING,
listing_id STRING,
longitude STRING,
original_timestamp STRING,
postal_code STRING,
region_id STRING,
review_status STRING,
sent_at STRING,
service_provider_id STRING,
sp_id STRING,
sp_name STRING,
timestamp STRING,
user_id STRING,
user_zip_code STRING,
uuid_ts STRING
)
PARTITIONED BY (edh_bus_date STRING)
LOCATION '${hivevar:S3_BUCKET}/data/incoming/events/angieslistweb_prod/incremental/daily/inc_angieslistweb_prod_review_submitted';