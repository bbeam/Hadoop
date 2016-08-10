
/* Turning off partition mode to non strict to allow dynamic partition without a single static partition. */
SET hive.exec.dynamic.partition.mode=non-strict;


/* Adding a record for the first run */
INSERT OVERWRITE TABLE common_operations.surrogate_key_map PARTITION(table_name) select 0, 'dim_product' FROM common_operations.initial_sk_setup LIMIT 1;
INSERT OVERWRITE TABLE common_operations.surrogate_key_map PARTITION(table_name) select 0, 'dim_market' FROM common_operations.initial_sk_setup LIMIT 1;


