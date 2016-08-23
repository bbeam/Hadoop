#########################################################################################
#                               General Details                                         #
#########################################################################################
# Name        : AngiesList                                                              #
# File        : wrapper_dim_load.sh                                                     #
# Description : This script loads data in Gold area dimension table.                    #
#               It reads from Work dimension table.                                     #
# Usage       : ./wrapper_cdc_pig_generator.sh global_property_file local_property_file##
#########################################################################################
#!/bin/bash

# FYI, SCRIPT_HOME will be /var/tmp/, if script is copied into /var/tmp/ and run from there.
SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Usage descriptor for invalid input arguments to the wrapper
Show_Usage()
{
    echo "invalid arguments please pass exactly three arguments "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path> <business date(YYYY-MM-DD)>"
    exit 1
}

if [ $# -ne 3 ]
then
    Show_Usage
fi

#./wrapper_cdc_pig_generator.sh 

# Assigning input arguments to proper variable names
GLOBAL_PROPERTY_FILE_PATH=$1
LOCAL_PROPERTY_FILE_PATH=$2
GLOBAL_PROPERTY_FILE_NAME=$(basename $GLOBAL_PROPERTY_FILE_PATH)
LOCAL_PROPERTY_FILE_NAME=$(basename $LOCAL_PROPERTY_FILE_PATH)


# Copy the global properties file (al-edh-global.properties) from S3 to HDFS and load the properties.
aws s3 cp $GLOBAL_PROPERTY_FILE_PATH /var/tmp/

if [ $? -eq 0 ]
then
    echo "global properties file " $GLOBAL_PROPERTY_FILE_NAME " copied from s3 successfully"
else
    echo "copy of global properties file " $GLOBAL_PROPERTY_FILE_NAME " from s3 failed"
    exit 1
fi

# Read global properties file
. /var/tmp/$GLOBAL_PROPERTY_FILE_NAME
if [ $? -eq 0 ]
then
    echo "global properties file " $GLOBAL_PROPERTY_FILE_NAME " read successfully"
else
    echo "global properties file " $GLOBAL_PROPERTY_FILE_NAME " read failed"
    exit 1
fi

# Copy the local properties (wrapper_cdc_pig_generator.properties) file from S3 to HDFS and load the properties.
aws s3 cp $LOCAL_PROPERTY_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
    echo "local properties file " $LOCAL_PROPERTY_FILE_NAME " copied from s3 successfully"
else
    echo "copy of local properties file " $LOCAL_PROPERTY_FILE_NAME " from s3 failed"
    exit 1
fi

# Read local properties file
. /var/tmp/$LOCAL_PROPERTY_FILE_NAME
if [ $? -eq 0 ]
then
    echo "local properties file " $LOCAL_PROPERTY_FILE_NAME " read successfully"
else
    echo "local properties file " $LOCAL_PROPERTY_FILE_NAME " read failed"
    exit 1
fi

LOAD_DIM_HIVE_FILE_NAME=$(basename $LOAD_DIM_HIVE_FILE_PATH)

# Copy .hql file to /var/tmp/ from s3
aws s3 cp $LOAD_DIM_HIVE_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
  echo "hive file copied from s3 to /var/tmp/ successfully"
else
  echo "copying hive file to s3 to /var/tmp/ failed"
  exit 1
fi

# Hive script to load target dimension table (in gold).
echo "hive script started running...doing target dimension load in gold"

hive -f /var/tmp/$LOAD_DIM_HIVE_FILE_NAME \
    -hivevar WORK_DIM_TABLE_NAME=$WORK_DIM_TABLE_NAME \
    -hivevar WORK_SHARED_DIM_DB=$WORK_SHARED_DIM_DB \
    -hivevar GOLD_SHARED_DIM_DB=$GOLD_SHARED_DIM_DB \
    -hivevar TRGT_DIM_TABLE_NAME=$TRGT_DIM_TABLE_NAME

if [ $? -eq 0 ]
then
    echo "hive script executed successfully. Dimension table load in target (gold area) completed "
else
    echo "hive execution failed. Dimension table load in target (gold area) process terminated"
    exit 1
fi

#========Updating maximum surrogate key in the ops_common.surrogate_key_map table========

#hive -e SET hive.exec.dynamic.partition.mode=non-strict; \
#INSERT OVERWRITE TABLE $OPERATIONS_COMMON_DB.surrogate_key_map PARTITION (table_name) \
#        SELECT MAX($SURROGATE_KEY) AS $SURROGATE_KEY, '${hivevar:TRGT_DIM_TABLE_NAME}' FROM ${hivevar:GOLD_SHARED_DIM_DB}.${hivevar:TRGT_DIM_TABLE_NAME};

hive -f $UPDATE_SURROGATE_KEY_HQL \
    -hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
    -hivevar SURROGATE_KEY=$SURROGATE_KEY \
    -hivevar TRGT_DIM_TABLE_NAME=$TRGT_DIM_TABLE_NAME \
    -hivevar GOLD_SHARED_DIM_DB=$GOLD_SHARED_DIM_DB

if [ $? -eq 0 ]
then
        echo "Max surrogate key updated"
else
        echo "Max surrogate key updation failed."
        exit 1
fi

 LOAD_DIM_AUDIT_HQL_FILE=$(basename $LOAD_DIM_AUDIT_HQL_PATH)

# Hive script to insert Dimension load audit record
 hive -f $LOAD_DIM_AUDIT_HQL_PATH \
    -hivevar ENTITY_NAME=$SUBJECT_SHAREDDIM \
    -hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
    -hivevar AUDIT_TABLE_NAME=$AUDIT_TABLE_NAME \
    -hivevar USER_NAME=$USER_NAME \
    -hivevar EDH_BUS_MONTH=$EDH_BUS_MONTH \
    -hivevar EDH_BUS_DATE=$EDH_BUS_DATE \
    -hivevar GOLD_DIM_DB=$GOLD_DIM_DB \
    -hivevar GOLD_DIM_TABLE=dim_market

# Hive Status check
if [ $? -eq 0 ]
then
        echo "$LOAD_DIM_AUDIT_HQL_FILE  executed without any error."
else
        echo "$LOAD_DIM_AUDIT_HQL_FILE  execution failed."
        exit 1
fi