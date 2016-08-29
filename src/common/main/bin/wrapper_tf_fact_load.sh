################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_tf_fact.sh                                             #
# Author      : Abhinav Mehar                                                  #
# Description : This Script performs transformation rules for event and load   # 
#               result set to the work_al_webmetrics.tf_fact_web_metrics table #
################################################################################

#!/bin/bash

# FYI, SCRIPT_HOME will be /var/tmp/, if script is copied into /var/tmp/ and run from there.
SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Usage descriptor for invalid input arguments to the wrapper
Show_Usage()
{
    echo "invalid arguments please pass exactly three arguments "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path> <YYYY-MM-DD>"
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
    echo "INFO:global properties file " $GLOBAL_PROPERTY_FILE_NAME " copied from s3 successfully"
else
    echo "ERROR:copy of global properties file " $GLOBAL_PROPERTY_FILE_NAME " from s3 failed"
    exit 1
fi

# Read global properties file
. /var/tmp/$GLOBAL_PROPERTY_FILE_NAME
if [ $? -eq 0 ]
then
    echo "INFO:global properties file " $GLOBAL_PROPERTY_FILE_NAME " read successfully"
else
    echo "ERROR:global properties file " $GLOBAL_PROPERTY_FILE_NAME " read failed"
    exit 1
fi

# Copy the local properties (wrapper_cdc_pig_generator.properties) file from S3 to HDFS and load the properties.
aws s3 cp $LOCAL_PROPERTY_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
    echo "INFO:local properties file " $LOCAL_PROPERTY_FILE_NAME " copied from s3 successfully"
else
    echo "ERROR:copy of local properties file " $LOCAL_PROPERTY_FILE_NAME " from s3 failed"
    exit 1
fi

# Read local properties file
. /var/tmp/$LOCAL_PROPERTY_FILE_NAME
if [ $? -eq 0 ]
then
    echo "INFO:local properties file " $LOCAL_PROPERTY_FILE_NAME " read successfully"
else
    echo "ERROR:local properties file " $LOCAL_PROPERTY_FILE_NAME " read failed"
    exit 1
fi

echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "EDH_BUS_DATE:$EDH_BUS_DATE"

# removal of existing data directories in work area
if hadoop fs -test -d $TF_TABLE_WORK_LOACTION; then

     hive -e "ALTER TABLE work_al_web_metrics.tf_nk_fact_web_metrics DROP PARTITION(event_type_key=$TF_EVENT)"
     if [ $? -eq 0 ]
     then
        echo "INFO:Dropping partition event_type_key=$TF_EVENT successful "
     else
        echo "ERROR:Dropping partition event_type_key=$TF_EVENT failed"
     exit 1
     fi

   echo "Removing transformation output data in work area for previous run.....Making $TF_DB.$TF_TABLE empty."
   hadoop fs -rmr $TF_TABLE_WORK_LOACTION
     if [ $? -eq 0 ]
     then
        echo "INFO:Making $TF_TABLE_WORK_LOACTION empty successful"
     else
        echo "ERROR:Making $TF_TABLE_WORK_LOACTION empty failed."
        exit 1
     fi
fi


# copy tf pig script to local
aws s3 cp $TF_PIG_FILE_PATH /var/tmp/

if [ $? -eq 0 ]
then
  echo "INFO:$TF_PIG_FILE_NAME file copied from s3 to /var/tmp/ successfully"
else
  echo "ERROR:copying $TF_PIG_FILE_NAME file to s3 to /var/tmp/ failed"
  exit 1
fi

TF_PIG_FILE_NAME=$(basename $TF_PIG_FILE_PATH)

# Pig Script to be triggered for transformation.
pig  \
    -param_file /var/tmp/$GLOBAL_PROPERTY_FILE_NAME \
    -param_file /var/tmp/$LOCAL_PROPERTY_FILE_NAME \
    -param EDH_BUS_DATE=$EDH_BUS_DATE \
    -file /var/tmp/$TF_PIG_FILE_NAME \
    -useHCatalog

if [ $? -eq 0 ]
then
        echo "INFO:$TF_PIG_FILE_NAME executed without any error."
else
        echo "ERROR:$TF_PIG_FILE_NAME execution failed."
        exit 1
fi

# Hive script to insert transformation audit record
hive -f $TF_AUDIT_HQL_PATH \
    -hivevar ENTITY_NAME=$SUBJECT_ALWEBMETRICS \
    -hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
    -hivevar AUDIT_TABLE_NAME=$AUDIT_TABLE_NAME \
    -hivevar USER_NAME=$USER_NAME \
    -hivevar TF_DB=$WORK_AL_WEB_METRICS_DB \
    -hivevar TF_EVENT=$TF_EVENT \
    -hivevar TF_EVENT_NAME=$TF_EVENT_NAME \
	-hivevar EDH_BUS_DATE=$EDH_BUS_DATE

# Hive Status check
if [ $? -eq 0 ]
then
        echo "INFO:$TF_AUDIT_HQL_PATH  executed without any error."
else
        echo "ERROR:$TF_AUDIT_HQL_PATH  execution failed."
        exit 1
fi
