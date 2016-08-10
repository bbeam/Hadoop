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

if [ "$#" -ne 2 ]; then
    echo "Provide 2 arguments in proper order as below:"
    echo "usage: .\wrapper_cdc_pig_generator GLOBAL_PROPERTY_FILE_PATH LOCAL_PROPERTY_FILE_PATH"
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


# Pig Script to be triggered for SCD.
echo "pig script started running...doing SCD"
pig \
	-param_file /var/tmp/$GLOBAL_PROPERTY_FILE_NAME \
	-param_file /var/tmp/$LOCAL_PROPERTY_FILE_NAME \
	-file $CDC_PIG_FILE_PATH \
	-useHCatalog
	
# Hive script to load target dimension table (in gold).
echo "hive script started running...doing target dimension load in gold"
hive -f $LOAD_DIM_HIVE_FILE_PATH \
	-hivevar GOLD_SHARED_DIM_DB=$SOURCE \
	-hivevar TRGT_DIM_TABLE_NAME=$INCOMING_DB \
	-hivevar WORK_SHARED_DIM_DB=$TABLE_NAME_INC \