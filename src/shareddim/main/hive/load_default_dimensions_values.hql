-- ######################################################################################################### 
-- HIVE SCRIPT				:load_default_dimesion_values.hql
-- AUTHOR					:Abhijeet Purwar
-- DESCRIPTION				:Hive script for laoding default valuse in dimenasion tables. It is intended for one time run
-- #########################################################################################################

INSERT INTO TABLE ${hivevar:GOLD_SHARED_DIM_DB}.dim_member
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, null, null, null, null, null, null, null, null, '${hivevar:STRING_MISSING_VALUE}', null, null, null, null, null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, null, null, null, null, null, null, null, null, '${hivevar:STRING_NA_VALUE}', null, null, null, null, null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, null, null, null, null, null, null, null, null, '${hivevar:STRING_UNKOWN_VALUE}', null, null, null, null, null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_SHARED_DIM_DB}.dim_market
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', ${hivevar:NUMERIC_MISSING_KEY}, null, null),
  (${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', ${hivevar:NUMERIC_NA_KEY},  null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:STRING_UNKOWN_VALUE}', ${hivevar:NUMERIC_UNKOWN_KEY}, null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_SHARED_DIM_DB}.dim_category
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', ${hivevar:NUMERIC_MISSING_VALUE}, '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', ${hivevar:NUMERIC_NA_VALUE}, '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', ${hivevar:NUMERIC_UNKOWN_VALUE}, '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', null, null);

INSERT INTO TABLE ${hivevar:GOLD_SHARED_DIM_DB}.dim_service_provider
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:STRING_UNKOWN_VALUE}', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_SHARED_DIM_DB}.dim_product
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', ${hivevar:NUMERIC_MISSING_VALUE}, '${hivevar:STRING_MISSING_VALUE}', null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', ${hivevar:NUMERIC_NA_VALUE}, '${hivevar:STRING_NA_VALUE}', null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', '${hivevar:STRING_UNKOWN_VALUE}', ${hivevar:NUMERIC_UNKOWN_VALUE}, '${hivevar:STRING_UNKOWN_VALUE}', null, null);