################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_tf_dim_market.sh                                       #
# Author      : Abhijeet Purwar                                                #
# Description : This Script performs the SCD process for dim market table      #
################################################################################

#!/bin/bash

# FYI, SCRIPT_HOME will be /var/tmp/, if script is copied into /var/tmp/ and run from there.
SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Usage descriptor for invalid input arguments to the wrapper
Show_Usage()
{
    echo "invalid arguments please pass exactly three arguments "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path> <yyyy-mm-dd>"
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

#echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "Business Date : $EDH_BUS_DATE"
#EDH_BUS_MONTH=$(date -d "$EDH_BUS_DATE" '+%Y%m')
#echo "Business Month :$EDH_BUS_MONTH"

UTC_TIME=`date +'%Y-%m-%d %H:%M:%S'`
EST_TIME=`TZ=":US/East-Indiana" date +'%Y-%m-%d %H:%M:%S'`

echo "UTC_TIME:$UTC_TIME"
echo "EST_TIME:$EST_TIME"

# hive to drop and re-create transformation work table
hive -f $CREATE_TF_HQL_PATH \
    -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
    -hivevar TF_TABLE_NAME=$TF_TABLE_NAME

if [ $? -eq 0 ]
then
  echo "Re-creation of $WORK_DIM_DB_NAME.$TF_TABLE_NAME in work area is successful"
else
  echo "Re-creation of $WORK_DIM_DB_NAME.$TF_TABLE_NAME in work area is failed"
  exit 1
fi

# hive to drop and re-create scd work table
hive -f $CREATE_SCD_HQL_PATH \
    -hivevar WORK_DIM_DB_NAME=$WORK_DIM_DB_NAME \
    -hivevar WORK_DIM_TABLE_NAME=$WORK_DIM_TABLE_NAME

if [ $? -eq 0 ]
then
  echo "Re-creation of $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME in work area is successful"
else
  echo "Re-creation of $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME in work area is failed"
  exit 1
fi

# removal of existing data directories in work area

if hadoop fs -test -f $TF_TABLE_WORK_LOACTION/part-m-00000; then
   echo "Removing transformation output data in work area for previous run.....Making $TF_DB.$TF_TABLE empty."
   hadoop fs -rm $TF_TABLE_WORK_LOACTION/*
     if [ $? -eq 0 ]
     then
        echo "Making $TF_DB.$TF_TABLE empty successful"
     else
        echo "Making $TF_DB.$TF_TABLE empty failed."
        exit 1
     fi
fi

if hadoop fs -test -f $TF_TABLE_WORK_LOACTION/part-r-00000; then
   echo "Removing transformation output data in work area for previous run.....Making $TF_DB.$TF_TABLE empty."
   hadoop fs -rm $TF_TABLE_WORK_LOACTION/*
     if [ $? -eq 0 ]
     then
        echo "Making $TF_DB.$TF_TABLE empty successful"
     else
        echo "Making $TF_DB.$TF_TABLE empty failed."
        exit 1
     fi
fi

if hadoop fs -test -f $CDC_TABLE_WORK_LOCATION/part-m-00000; then
   echo "Removing cdc output data in work are for previous run.....Making $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME empty."
   hadoop fs -rm $CDC_TABLE_WORK_LOCATION/*
     if [ $? -eq 0 ]
     then
        echo "Making $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME empty successful"
     else
        echo "Making $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME empty failed."
        exit 1
     fi
fi

if hadoop fs -test -f $CDC_TABLE_WORK_LOCATION/part-r-00000; then
   echo "Removing cdc output data in work are for previous run.....Making $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME empty."
   hadoop fs -rm $CDC_TABLE_WORK_LOCATION/*
     if [ $? -eq 0 ]
     then
        echo "Making $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME empty successful"
     else
        echo "Making $WORK_DIM_DB_NAME.$WORK_DIM_TABLE_NAME empty failed."
        exit 1
     fi
fi


# copy tf pig script to local
aws s3 cp $TF_PIG_FILE_PATH /var/tmp/

if [ $? -eq 0 ]
then
  echo "$TF_PIG_FILE_NAME file copied from s3 to /var/tmp/ successfully"
else
  echo "copying $TF_PIG_FILE_NAME file to s3 to /var/tmp/ failed"
  exit 1
fi

TF_PIG_FILE_NAME=$(basename $TF_PIG_FILE_PATH)

# Pig Script to be triggered for transformation.
pig  \
    -param_file /var/tmp/$GLOBAL_PROPERTY_FILE_NAME \
    -param_file /var/tmp/$LOCAL_PROPERTY_FILE_NAME \
    -param UTC_TIME="$UTC_TIME" \
    -param EST_TIME="$EST_TIME" \
    -file /var/tmp/$TF_PIG_FILE_NAME \
    -useHCatalog

if [ $? -eq 0 ]
then
        echo "$TF_PIG_FILE_NAME executed without any error."
else
        echo "$TF_PIG_FILE_NAME execution failed."
        exit 1
fi

# Hive script to insert transformation audit record
hive -f $TF_AUDIT_HQL_PATH \
    -hivevar ENTITY_NAME=$SUBJECT_SHAREDDIM \
    -hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
    -hivevar AUDIT_TABLE_NAME=$AUDIT_TABLE_NAME \
    -hivevar USER_NAME=$USER_NAME \
    -hivevar TF_DB=$TF_DB \
    -hivevar TF_TABLE=$TF_TABLE \
    -hivevar EDH_BUS_DATE=$EDH_BUS_DATE

# Hive Status check
if [ $? -eq 0 ]
then
        echo "$TF_AUDIT_HQL_PATH  executed without any error."
else
        echo "$TF_AUDIT_HQL_PATH  execution failed."
        exit 1
fi

echo "*****************CDC PROCESS STARTS*******************"

# Copy .pig file to /var/tmp/ from s3
aws s3 cp $CDC_PIG_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
  echo "pig file copied from s3 to /var/tmp/ successfully"
else
  echo "copying pig file from s3 to /var/tmp/ failed"
  exit 1
fi

CDC_PIG_FILE_NAME=$(basename $CDC_PIG_FILE_PATH)
LOAD_DIM_HIVE_FILE_NAME=$(basename $LOAD_DIM_HIVE_FILE_PATH)


# Pig Script to be triggered for SCD.
echo "pig script started running...doing SCD"
pig \
    -param_file /var/tmp/$GLOBAL_PROPERTY_FILE_NAME \
    -param_file /var/tmp/$LOCAL_PROPERTY_FILE_NAME \
    -param UTC_TIME="$UTC_TIME" \
    -param EST_TIME="$EST_TIME" \
    -file /var/tmp/$CDC_PIG_FILE_NAME \
    -useHCatalog

if [ $? -eq 0 ]
then
    echo "pig script executed successfully. SCD process completed"
else
    echo "pig execution failed. SCD process terminated."
    exit 1
fi

# Hive script to insert CDC audit record
hive -f $CDC_AUDIT_HQL_PATH \
    -hivevar ENTITY_NAME=$SUBJECT_SHAREDDIM \
    -hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
    -hivevar AUDIT_TABLE_NAME=$AUDIT_TABLE_NAME \
    -hivevar USER_NAME=$USER_NAME \
    -hivevar WORK_CDC_DB=$WORK_DIM_DB_NAME \
    -hivevar WORK_CDC_TABLE=$WORK_DIM_TABLE_NAME
	-hivevar EDH_BUS_DATE=$EDH_BUS_DATE
# Hive Status check
if [ $? -eq 0 ]
then
        echo "$CDC_AUDIT_HQL_PATH  executed without any error."
 else
        echo "$CDC_AUDIT_HQL_PATH  execution failed."
        exit 1
fi
