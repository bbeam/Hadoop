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

# Copy the shareddim.properties file from S3 to EMR
aws s3 cp s3://al-edh-dm/src/shareddim/main/conf/shareddim.properties $SCRIPT_HOME/ 2> /dev/null
if [ $? -eq 0 ]
then
  echo shareddim.properties copied successfully to $SCRIPT_HOME
else
  echo shareddim.properties copy to $SCRIPT_HOME failed >&2
  echo "$1" 1>&2
  exit 1
fi

# Read the shareddim.properties file
. $SCRIPT_HOME/shareddim.properties 2> /dev/null
if [ $? -eq 0 ]
then
  echo shareddim.properties read successfully
else
  echo shareddim.properties read failed >&2
  echo "$1" 1>&2
  exit 1
fi

# Copy jar file from s3 to EMR 
aws s3 cp $DIM_DATE_JAR_FILE_LOCATION_S3 $SCRIPT_HOME/ 2> /dev/null
if [ $? -eq 0 ]
then
  echo "$DIM_DATE_JAR_FILE_NAME copied from  $DIM_DATE_JAR_FILE_LOCATION_S3 to $SCRIPT_HOME"
else
  echo "$DIM_DATE_JAR_FILE_NAME copy from  $DIM_DATE_JAR_FILE_LOCATION_S3  to  $SCRIPT_HOME failed" >&2
  echo "$1" 1>&2
  exit 1
fi

# Run Dim_Date.jar to create csv file in EMR
java -jar $SCRIPT_HOME/$DIM_DATE_JAR_FILE_NAME $SCRIPT_HOME/$DIM_DATE_EXTERNAL_HIVE_FILE_NAME $DIM_DATE_STARTDATE $DIM_DATE_1ST_DAY_OF_BUIS_WK 2> /dev/null
if [ $? -eq 0 ] 2> /dev/null
then
  echo "$DIM_DATE_JAR_FILE_NAME executed successfully and date dimension $DIM_DATE_EXTERNAL_HIVE_FILE_NAME file created locally"
else
  echo "$DIM_DATE_JAR_FILE_NAME execution failed" >&2
  echo "$1" 1>&2
  exit 1
fi

# Copy csv file in EMR to s3
aws s3 cp $SCRIPT_HOME/$DIM_DATE_EXTERNAL_HIVE_FILE_NAME $DIM_DATE_HIVE_TABLE_LOCATION_S3 2> /dev/null
if [ $? -eq 0 ] 2> /dev/null
then
  echo "$DIM_DATE_EXTERNAL_HIVE_FILE_NAME copied successfully from  $SCRIPT_HOME  to  $DIM_DATE_HIVE_TABLE_LOCATION_S3"
else
  echo "$DIM_DATE_EXTERNAL_HIVE_FILE_NAME copy from $SCRIPT_HOME to $DIM_DATE_HIVE_TABLE_LOCATION_S3 failed" >&2
  echo "$1" 1>&2
  exit 1
fi

# Run hql to create external hive table for Dim_Date
hive -S -hivevar DIM_DATE_DATABASE_NAME=$DIM_DATE_DATABASE_NAME -hivevar DIM_DATE_TABLE_NAME=$DIM_DATE_TABLE_NAME -hivevar DIM_DATE_HIVE_TABLE_LOCATION_S3=$DIM_DATE_HIVE_TABLE_LOCATION_S3 -f $DIM_DATE_HIVE_SCRIPT_LOCATION_S3 2> /dev/null
if [ $? -eq 0 ] 2> /dev/null
then
  echo "$DIM_DATE_TABLE_NAME created at external location $DIM_DATE_HIVE_TABLE_LOCATION_S3"
else
  echo "$DIM_DATE_TABLE_NAME creation failed " >&2
  echo "$1" 1>&2
  exit 1
fi
exit 0
