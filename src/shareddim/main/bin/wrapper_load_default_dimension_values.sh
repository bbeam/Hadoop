################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_load_default_dimension_values.sh                                       #
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
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path>"
    exit 1
}

if [ $# -ne 2 ]
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

hive -f $LOAD_DEFAULT_DIM_VALUES_HQL \
    -hivevar GOLD_DIM_DB=$GOLD_DIM_DB \
    -hivevar NUMERIC_MISSING_KEY=$NUMERIC_MISSING_KEY \
    -hivevar NUMERIC_NA_KEY=$NUMERIC_NA_KEY \
    -hivevar NUMERIC_UNKOWN_KEY=$NUMERIC_UNKOWN_KEY \
    -hivevar STRING_NA_VALUE=$STRING_NA_VALUE \
    -hivevar STRING_MISSING_VALUE=$STRING_MISSING_VALUE

if [ $? -eq 0 ]
then
        echo "Defalut values loaded successfully in dimension tables."
 else
        echo "Loading default values in dimension tables failed."
        exit 1
fi