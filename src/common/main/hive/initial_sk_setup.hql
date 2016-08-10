CREATE EXTERNAL TABLE IF NOT EXISTS common_operations.initial_sk_setup
(
zero_val TINYINT
)
LOCATION 's3://al-edh-dev/data/operations/common/initial_sk_setup';

INSERT INTO TABLE common_operations.initial_sk_setup values (0);