SET hive.exec.dynamic.partition.mode=non-strict;
INSERT OVERWRITE TABLE ${hivevar:OPERATIONS_COMMON_DB}.surrogate_key_map PARTITION (table_name)
        SELECT MAX(${hivevar:SURROGATE_KEY}) AS ${hivevar:SURROGATE_KEY}, '${hivevar:TRGT_DIM_TABLE_NAME}' FROM ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME};
