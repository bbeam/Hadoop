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

# Calculating business month from business date. Business date is received from datapipeline current_date-1
echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "Business Date : $EDH_BUS_DATE"
EDH_BUS_MONTH=$(date -d "$EDH_BUS_DATE" '+%Y%m')
echo "Business Month :$EDH_BUS_MONTH"

# Pig Script to be triggered for SCD.
echo "pig script started running...doing SCD"
pig \
    -param EDH_BUS_MONTH=$EDH_BUS_MONTH \
    -param_file /var/tmp/$GLOBAL_PROPERTY_FILE_NAME \
    -param_file /var/tmp/$LOCAL_PROPERTY_FILE_NAME \
    -file $CDC_PIG_FILE_PATH \
    -useHCatalog

if [ $? -eq 0 ]
then
    echo "pig script executed successfully. SCD process completed"
else
    echo "pig execution failed. SCD process terminated."
    exit 1
fi

# Hive script to load target dimension table (in gold).
echo "hive script started running...doing target dimension load in gold"
hive -f $LOAD_DIM_HIVE_FILE_PATH \
    -hivevar GOLD_SHARED_DIM_DB=$SOURCE \
    -hivevar TRGT_DIM_TABLE_NAME=$INCOMING_DB \
    -hivevar WORK_SHARED_DIM_DB=$TABLE_NAME_INC \

if [ $? -eq 0 ]
then
    echo "hive script executed successfully. Dimension table load in target (gold area) completed "
else
    echo "hive execution failed. Dimension table load in target (gold area) process terminated"
    exit 1
fi