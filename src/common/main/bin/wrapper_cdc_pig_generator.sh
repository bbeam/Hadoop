#########################################################################################
#                               General Details                                         #
#########################################################################################
# Name        : AngiesList                                                              #
# File        : wrapper_cdc_pig_generator.sh                                            #
# Description : This script runs java jar utility to generate pig script which          #
#               will do SCD for dimension tables. It will also generate                 #
#               hive script for target (gold) dimension table load and updates          #
#               maximumu surrogate key value in alwebmetrics_Gold.sk_map                #
#               table.                                                                  #
#               The generated pig scipt and hive script should be run as a part         #
#               of one wrapper script.                                                  #
# Author      : Abhijeet Purwar                                                         #
# Usage       : ./wrapper_cdc_pig_generator.sh global_property_file local_property_file##
#########################################################################################

#!/bin/bash

# FYI, SCRIPT_HOME will be /var/tmp/, if script is copied into /var/tmp/ and run from there.
SCRIPT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Usage descriptor for invalid input arguments to the wrapper
Show_Usage()
{
    echo "Invalid arguments. Please provide 2 arguments in exactly as below: "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path>"
    exit 1
}

if [ $# -ne 2 ]
then
    Show_Usage
fi

# Assigning input arguments to proper variable names
GLOBAL_PROPERTY_FILE_PATH=$1
LOCAL_PROPERTY_FILE_PATH=$2
GLOBAL_PROPERTY_FILE_NAME=$(basename $GLOBAL_PROPERTY_FILE_PATH)
LOCAL_PROPERTY_FILE_NAME=$(basename $LOCAL_PROPERTY_FILE_PATH)


# Copy the global properties file (al-edh-global.properties) from S3 to HDFS and load the properties.
aws s3 cp $GLOBAL_PROPERTY_FILE_PATH /var/tmp/

if [ $? -eq 0 ]
then
    echo "global properties file " $GLOBAL_PROPERTY_FILE_NAME " copied from s3 succecfully"
else
    echo "copy of global properties file " $GLOBAL_PROPERTY_FILE_NAME " from s3 failed"
    exit 1
fi

# Read global properties file
. /var/tmp/$GLOBAL_PROPERTY_FILE_NAME
if [ $? -eq 0 ]
then
    echo "global properties file " $GLOBAL_PROPERTY_FILE_NAME " read succecfully"
else
    echo "global properties file " $GLOBAL_PROPERTY_FILE_NAME " read failed"
    exit 1
fi

# Copy the local properties (wrapper_cdc_pig_generator.properties) file from S3 to HDFS and load the properties.
aws s3 cp $LOCAL_PROPERTY_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
    echo "local properties file " $LOCAL_PROPERTY_FILE_NAME " copied from s3 succecfully"
else
    echo "copy of local properties file " $LOCAL_PROPERTY_FILE_NAME " from s3 failed"
    exit 1
fi

# Read local properties file
. /var/tmp/$LOCAL_PROPERTY_FILE_NAME
if [ $? -eq 0 ]
then
    echo "local properties file " $LOCAL_PROPERTY_FILE_NAME " read succecfully"
else
    echo "local properties file " $LOCAL_PROPERTY_FILE_NAME " read failed"
    exit 1
fi

JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)
INPUT_JSON_FILE_NAME=$(basename $INPUT_JSON_FILE_PATH)
JSON_SCHEMA_FILE_NAME=$(basename $VALIDATION_SCHEMA_FILE_PATH)
CDC_PIG_FILE_NAME=$(basename $CDC_PIG_FILE_PATH)
LOAD_DIM_HIVE_FILE_NAME=$(basename $LOAD_DIM_HIVE_FILE_PATH)

# Copy JSON schema for input JSON validation
aws s3 cp $VALIDATION_SCHEMA_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
    echo "JSON schema file for validation copied succecfully"
else
    echo " copy of JSON schema file for validation failed"
    exit 1
fi

# Copy JSON file
aws s3 cp $INPUT_JSON_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
    echo "JSON input file copied succecfully"
else
    echo " copy of JSON input file failed"
    exit 1
fi

# Copy CDC_PIG_GENERATOR_JAR file
aws s3 cp $DQ_GENERATOR_JAR_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
    echo "$JAR_FILE_NAME file copied succecfully"
else
    echo " copy of $JAR_FILE_NAME file failed"
    exit 1
fi


# Run java jar program for to generate pig and hql files for SCD
#java -jar /var/tmp/$JAR_FILE_NAME /var/tmp/$INPUT_JSON_FILE_NAME /var/tmp/$JSON_SCHEMA_FILE_NAME
java -cp /var/tmp/EDH_JAVA-1.0-jar-with-dependencies.jar com.angieslist.edh.scd.scdgenerator.SCDPigGenerator /var/tmp/$INPUT_JSON_FILE_NAME /var/tmp/$JSON_SCHEMA_FILE_NAME

if [ $? -eq 0 ]
then
  echo ".pig for doing scd created successfully"
else
  echo "$pig generation failed"
  exit 1
fi

# Copy output .pig file to s3
aws s3 cp $CDC_PIG_FILE_NAME $CDC_PIG_FILE_PATH
if [ $? -eq 0 ]
then
  echo "pig file copied to s3 successfully"
else
  echo "copying pig file to s3 failed"
  exit 1
fi

# Copy output hive .hql file to s3
aws s3 cp $LOAD_DIM_HIVE_FILE_NAME $LOAD_DIM_HIVE_FILE_PATH
if [ $? -eq 0 ]
then
  echo "hive file copied to s3 successfully"
else
  echo "copying hive file to s3 failed"
  exit 1
fi