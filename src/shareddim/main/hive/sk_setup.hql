-- AUTHOR					:Abhijeet Purwar
-- support table for initial values (zero) generation for surrogate_key of all dimensions
CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup
(
zero_val TINYINT
)
LOCATION '${hivevar:S3_BUCKET}/data/operations/common/initial_sk_setup';

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup values (0);