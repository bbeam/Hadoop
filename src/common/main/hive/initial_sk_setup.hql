CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup
(
zero_val TINYINT
)
LOCATION 's3://al-edh-dev/data/operations/common/initial_sk_setup';

<<<<<<< HEAD
INSERT INTO TABLE ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup values (0);
=======
INSERT INTO TABLE ops_common.initial_sk_setup values (0);
>>>>>>> f4b0034217bdf9fd1e215b17687167acdf5184a7
