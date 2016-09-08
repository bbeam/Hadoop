################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_tf_wm_sk_fact_load.sh                                     #
# Author      : Abhinav Mehar                                                  #
# Description : This Script performs transformation rules for event and load   # 
#               result set to the work_al_webmetrics.tf_sk_fact_web_metrics table #
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

if [ $# -ne 2 ]
then
    Show_Usage
fi

#./wrapper_cdc_pig_generator.sh 

# Assigning input arguments to proper variable names
GLOBAL_PROPERTY_FILE_PATH=$1
GLOBAL_PROPERTY_FILE_NAME=$(basename $GLOBAL_PROPERTY_FILE_PATH)


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

echo "****************BUSINESS DATE/MONTH*****************"
EDH_BUS_DATE=$3
echo "EDH_BUS_DATE:$EDH_BUS_DATE"


# Hive script to insert transformed records to the final gold db table
hive -f $TF_AUDIT_HQL_PATH \
    -hivevar GOLD_AL_WEB_METRICS_DB=$GOLD_AL_WEB_METRICS_DB \
    -hivevar WORK_AL_WEB_METRICS_DB=$WORK_AL_WEB_METRICS_DB

# Hive Status check
if [ $? -eq 0 ]
then
        echo "INFO:$TF_AUDIT_HQL_PATH  executed without any error."
else
        echo "ERROR:$TF_AUDIT_HQL_PATH  execution failed."
        exit 1
fi
