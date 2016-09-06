
/* Turning off partition mode to non strict to allow dynamic partition without a single static partition. */
SET hive.exec.dynamic.partition.mode=non-strict;


/* Adding a record for the first run */
INSERT OVERWRITE TABLE ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map PARTITION(table_name) select 0, 'dim_product' FROM ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup LIMIT 1;
INSERT OVERWRITE TABLE ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map PARTITION(table_name) select 0, 'dim_market' FROM ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup LIMIT 1;
INSERT OVERWRITE TABLE ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map PARTITION(table_name) select 0, 'dim_category' FROM ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup LIMIT 1;
INSERT OVERWRITE TABLE ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map PARTITION(table_name) select 0, 'dim_member' FROM ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup LIMIT 1;
INSERT OVERWRITE TABLE ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map PARTITION(table_name) select 0, 'dim_service_provider' FROM ${hivevar:OPERATIONS_COMMON_DB}.initial_sk_setup LIMIT 1;
