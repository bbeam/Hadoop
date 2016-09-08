INSERT INTO TABLE ${hivevar:GOLD_DIM_DB}.dim_member
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, null, null, null, null, null, null, null, null, '${hivevar:STRING_MISSING_VALUE}', null, null, null, null, null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, null, null, null, null, null, null, null, null, '${hivevar:STRING_NA_VALUE}', null, null, null, null, null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, null, null, null, null, null, null, null, null, '${hivevar:NUMERIC_UNKOWN_KEY}', null, null, null, null, null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_DIM_DB}.dim_market
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', ${hivevar:NUMERIC_MISSING_KEY}, null, null),
  (${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', ${hivevar:NUMERIC_NA_KEY},  null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:NUMERIC_UNKOWN_KEY}', ${hivevar:NUMERIC_UNKOWN_KEY}, null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_DIM_DB}.dim_category
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '0', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '0', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', '0', '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_DIM_DB}.dim_event_type
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}'),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}'),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:NUMERIC_UNKOWN_KEY}', ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:NUMERIC_UNKOWN_KEY}');
  
INSERT INTO TABLE ${hivevar:GOLD_DIM_DB}.dim_service_provider
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:NUMERIC_UNKOWN_KEY}', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
  
INSERT INTO TABLE ${hivevar:GOLD_DIM_DB}.dim_product
  VALUES (${hivevar:NUMERIC_MISSING_KEY}, ${hivevar:NUMERIC_MISSING_KEY}, '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', '${hivevar:STRING_MISSING_VALUE}', 0, '${hivevar:STRING_MISSING_VALUE}', null, null),
  (${hivevar:NUMERIC_NA_KEY}, ${hivevar:NUMERIC_NA_KEY}, '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', '${hivevar:STRING_NA_VALUE}', 0, '${hivevar:STRING_NA_VALUE}', null, null),
  (${hivevar:NUMERIC_UNKOWN_KEY}, ${hivevar:NUMERIC_UNKOWN_KEY}, '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', '${hivevar:NUMERIC_UNKOWN_KEY}', 0, '${hivevar:NUMERIC_UNKOWN_KEY}', null, null);