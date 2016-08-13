CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup
(
zero_val TINYINT
)
LOCATION 's3://al-edh-dev/data/operations/common/initial_sk_setup';

INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup values (0);

