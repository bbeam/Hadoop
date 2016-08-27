################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_tf_fact_setup.sh                                             #
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

if [ $# -ne 1 ]
then
    Show_Usage
fi


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


# removal of existing data directories in work area

if hadoop fs -test -d $WORK_DIR/data/work/alwebmetrics/tf_nk_fact_webmetrics; then
   echo "Removing transformation output data in work area for previous run.....Making $WORK_AL_WEBMETRICS_DB.tf_nk_fact_webmetrics empty."
   hadoop fs -rmr $WORK_DIR/data/work/alwebmetrics/tf_nk_fact_webmetrics
     if [ $? -eq 0 ]
     then
        echo "INFO:Making $WORK_AL_WEBMETRICS_DB.tf_nk_fact_webmetrics empty successful"
     else
        echo "ERROR:Making $WORK_AL_WEBMETRICS_DB.tf_nk_fact_webmetrics empty failed."
        exit 1
     fi
fi

# hive to drop and re-create transformation work table
hive -f $S3_BUCKET/src/alwebmetrics/main/hive/create_tf_nk_fact_webmetrics.hql \
    -hivevar WORK_AL_WEBMETRICS_DB=$WORK_AL_WEBMETRICS_DB \
    -hivevar WORK_DIR=$WORK_DIR

if [ $? -eq 0 ]
then
  echo "Re-creation of $WORK_AL_WEBMETRICS_DB.tf_nk_fact_webmetrics in work area is successful"
else
  echo "Re-creation of $WORK_AL_WEBMETRICS_DB.tf_nk_fact_webmetrics in work area is failed"
  exit 1
fi