################################################################################
#                               General Details                                #
####################################################################################
# Name        : AngiesList                                                     	   #
# File        : wrapper_dq_generator.sh           	                           	   #
# Description : This shell script calls a java utility class which takes into  	   #
#				json file as input, creates the dq pig output and stores into  	   #
#				S3 location.												   	   #
# Author      : Varun Rauthan                                                  	   #
# Usage       : ./wrapper_dq_generator.sh                      				   	   #
#             : for example: 												   	   #
# ./wrapper_dq_generator.sh <GLOBAL_PROPERTY_FILE_PATH> <LOCAL_PROPERTY_FILE_PATH> #
####################################################################################

# Check for the input parameters to the shell script. The script only expects global properties as first parameter and local parameter as second parameter.
Show_Usage()
{
    echo "invalid arguments please pass two arguments "
    echo "Usage: "$0" <global properties file with path> <path of local properties file with path>"
    exit 1
}

if [ $# -ne 2 ] 
then
    Show_Usage
fi

# Assigning input arguments to proper variable names
GLOBAL_PROPERTY_FILE_PATH=$1
LOCAL_PROPERTY_FILE_PATH="s3://al-edh-dev/src/alweb/main/conf/dq_t_sku.properties"
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

JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)
INPUT_JSON_FILE_NAME=$(basename $INPUT_JSON_FILE_PATH)
SCHEMA_FILE_NAME=$(basename $VALIDATION_SCHEMA_FOR_INPUT_JSON)
OUTPUT_PIG_FILE_NAME=$(basename $OUTPUT_PIG_FILE_PATH)
JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)

# Copy JSON schema for input JSON validation
aws s3 cp $VALIDATION_SCHEMA_FOR_INPUT_JSON /var/tmp/
if [ $? -eq 0 ]
then
	echo "JSON schema file for validation copied successfully"
else
	echo " copy of JSON schema file for validation failed"
	exit 1
fi

# Copy JSON file
aws s3 cp $INPUT_JSON_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
	echo "JSON input file copied successfully"
else
	echo " copy of JSON input file failed"
	exit 1
fi

# Copy JAR file
aws s3 cp $DQ_GENERATOR_JAR_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
	echo "JSON input file copied successfully"
else
	echo " copy of JSON input file failed"
	exit 1
fi

# Run java jar program for to generate pig and hql files for SCD
java -jar /var/tmp/$JAR_FILE_NAME /var/tmp/$INPUT_JSON_FILE_NAME /var/tmp/$SCHEMA_FILE_NAME
java -cp /var/tmp/EDH_JAVA-1.0-jar-with-dependencies.jar com.angieslist.edh.dq.dqgenerator.DQParser  /var/tmp/$INPUT_JSON_FILE_NAME /var/tmp/$SCHEMA_FILE_NAME


if [ $? -eq 0 ]
then
  echo ".pig for doing scd created successfully"
else
  echo "$pig generation failed"
  exit 1
fi

# Copy output .pig file to s3
aws s3 cp $OUTPUT_PIG_FILE_NAME $OUTPUT_PIG_FILE_PATH
if [ $? -eq 0 ]
then
  echo "pig file copied to s3"
else
  echo "copying pig file to s3 failed"
  exit 1
fi