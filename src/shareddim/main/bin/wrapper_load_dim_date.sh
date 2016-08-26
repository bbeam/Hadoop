################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : Wrapper_Dim_Date.sh                                            #
# Description : This script runs java jar utility to create date dimension.    #
#               Also creates hive external table to point to s3 location.      #
#               It is one time run activity.                                   #
# Author      : Abhijeet Purwar                                                #
################################################################################

#!/bin/bash

# FYI, SCRIPT_HOME will be /var/tmp/, if script is copied into /var/tmp/ and run from there.
SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Usage descriptor for invalid input arguments to the wrapper
Show_Usage()
{
    echo "invalid arguments please pass exactly three arguments "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path> <start_date> <edh_bus_date for audit>"
    exit 1
}

if [ $# -ne 4 ]
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

echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$4
echo "Business Date : $EDH_BUS_DATE"
#EDH_BUS_MONTH=$(date -d "$EDH_BUS_DATE" '+%Y%m')
#echo "Business Month :$EDH_BUS_MONTH"

# Copy jar file from s3 to EMR 
aws s3 cp $DIM_DATE_JAR_FILE_LOCATION_S3 /var/tmp/
if [ $? -eq 0 ]
then
  echo "$DIM_DATE_JAR_FILE_NAME copied from  $DIM_DATE_JAR_FILE_LOCATION_S3 to /var/tmp/"
else
  echo "$DIM_DATE_JAR_FILE_NAME copy from  $DIM_DATE_JAR_FILE_LOCATION_S3  to  /var/tmp/ failed"
  exit 1
fi

DIM_DATE_JAR_FILE_NAME=$(basename $DIM_DATE_JAR_FILE_LOCATION_S3)

# Run Dim_Date.jar to create csv file in EMR
java -jar /var/tmp/$DIM_DATE_JAR_FILE_NAME /var/tmp/$DIM_DATE_EXTERNAL_HIVE_FILE_NAME $DIM_DATE_STARTDATE $DIM_DATE_1ST_DAY_OF_BUIS_WK
if [ $? -eq 0 ]
then
  echo "$DIM_DATE_JAR_FILE_NAME executed successfully and date dimension $DIM_DATE_EXTERNAL_HIVE_FILE_NAME file created locally"
else
  echo "$DIM_DATE_JAR_FILE_NAME execution failed"
  exit 1
fi

# Copy csv file in EMR to s3
aws s3 cp /var/tmp/$DIM_DATE_EXTERNAL_HIVE_FILE_NAME $DIM_DATE_HIVE_TABLE_LOCATION_S3
if [ $? -eq 0 ]
then
  echo "$DIM_DATE_EXTERNAL_HIVE_FILE_NAME copied successfully from /var/tmp/ to  $DIM_DATE_HIVE_TABLE_LOCATION_S3"
else
  echo "$DIM_DATE_EXTERNAL_HIVE_FILE_NAME copy from /var/tmp/ to $DIM_DATE_HIVE_TABLE_LOCATION_S3 failed"
  exit 1
fi

 LOAD_DIM_AUDIT_HQL_FILE=$(basename $LOAD_DIM_AUDIT_HQL_PATH)

# Hive script to insert Dimension load audit record
 hive -f $LOAD_DIM_AUDIT_HQL_PATH \
    -hivevar ENTITY_NAME=$SUBJECT_SHAREDDIM \
    -hivevar OPERATIONS_COMMON_DB=$OPERATIONS_COMMON_DB \
    -hivevar AUDIT_TABLE_NAME=$AUDIT_TABLE_NAME \
    -hivevar USER_NAME=$USER_NAME \
    -hivevar EDH_BUS_DATE=$EDH_BUS_DATE \
    -hivevar GOLD_DIM_DB=$GOLD_SHARED_DIM_DB \
    -hivevar GOLD_DIM_TABLE=$TRGT_DIM_TABLE_NAME

# Hive Status check
if [ $? -eq 0 ]
then
        echo "$LOAD_DIM_AUDIT_HQL_FILE  executed without any error."
else
        echo "$LOAD_DIM_AUDIT_HQL_FILE  execution failed."
        exit 1
fi