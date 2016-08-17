SET hive.exec.dynamic.partition.mode=non-strict;
-- ######################################################################################################### 
-- HIVE SCRIPT                :targetdim_market_load.hql
-- AUTHOR                    :Hive script is auto generated with java utility.
-- DESCRIPTION                :Hive script will run after pig script for scd.
--                          Hive script will load data from work table to target dim_market table.
--                          Hive sscript will also update common_operations.surrogate_key_map for maximum surrogate key generated.
--                         This pig script and hive script are part of one wrapper script named: wrapper_cdc_target_dim_load.sh
-- #########################################################################################################


use ${hivevar:GOLD_SHARED_DIM_DB};

-- =========Loading work table to target dimension table========
INSERT OVERWRITE TABLE ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME} PARTITION (EDH_BUS_MONTH)
SELECT market_key,
        market_id,
        market_nm,
        EDH_BUS_MONTH
 FROM ${hivevar:WORK_SHARED_DIM_DB}.${hivevar:WORK_DIM_TABLE_NAME};
