################################################################################
#                               General Details                                #
################################################################################
# Name        : AngiesList                                                     #
# File        : wrapper_dq_t_sku.sh                                            #
# Description : This script performs data quality and cleansing on the data,   #
#				and finally creates a new table with the elements according to #
#				the incoming schema							 				   #
################################################################################


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

# Copy the al-edh-global.properties file from S3 to local and load the properties.
global_file=`echo "$(basename $1)"`
aws s3 cp $1 /var/tmp/

if [ $? -eq 0 ]
then
        echo "${global_file} file copied successfully from "$1" to "/var/tmp/" "
else
   		echo "Copy of ${global_file} failed from $1."
    	exit 1
fi

. /var/tmp/${global_file}
if [ $? -eq 0 ]
then
        echo "Load of ${global_file} successful"
else
		echo "Load of ${global_file} failed"
		exit 1
fi

# Copy the local properties file from S3 to local and load the properties.
local_file=`echo "$(basename $2)"`
if [ $? -eq 0 ]
then
		aws s3 cp $2 /var/tmp/
else
   		echo "Copy of ${local_file} failed from $1."
    	exit 1
fi

. /var/tmp/${local_file}
if [ $? -eq 0 ]
then
        echo "Load of ${local_file} successful"
else
		echo "Load of ${local_file} failed"
		exit 1
fi

echo "****************BUSINESS DATE*****************"
BUS_DATE=$3
echo "Business Date : $BUS_DATE"

echo "*************SQOOP IMPORT JOB UTILITY*******************"
echo -e "sqoop import --connect $CONNECTION_URL --username $USERNAME --password $PASSWORD  --target-dir $S3_BUCKET/$DATA_DIRECTORY=$BUS_DATE --options-file $OPTIONS_FILE_NAME"
cat $OPTIONS_FILE_NAME 

sqoop import --connect $CONNECTION_URL --username $USERNAME --password $PASSWORD  --target-dir $S3_BUCKET/$DATA_DIRECTORY=$BUS_DATE --options-file $OPTIONS_FILE_NAME

if [ $? -eq 0 ]
then
  		echo "Successfully Ingested the data into s3 directory $S3_BUCKET/$BUCKET_DIRECTORY=$BUS_DATE"
else
  		echo "Sqoop Ingestion Job is Failed" >&2
  		exit 1
fi

# Hive Metastore refresh for incoming table as it is a manual partitioned table .
hive -e "msck repair table $ALWEB_INCOMING_DB.$TABLE_NAME_INC"

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for incoming table."
else
  		echo "Hive Metastore refresh failed for incoming table." >&2
  		exit 1
fi

# Hive Metastore refresh for error table .
hive -e "msck repair table ${COMMON_OPERATIONS_DB}.${ERROR_TABLE_NAME}

# Hive Metastore refresh status check
if [ $? -eq 0 ]
then
		echo "Hive Metastore refresh successful for error table."
else
  		echo "Hive Metastore refresh failed for error table" >&2
  		exit 1
fi

echo "*****************DQ PROCESS STARTS*******************"
JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)
INPUT_JSON_FILE_NAME=$(basename $INPUT_JSON_FILE_PATH)
SCHEMA_FILE_NAME=$(basename $VALIDATION_SCHEMA_FOR_INPUT_JSON)
OUTPUT_PIG_FILE_NAME=$(basename $OUTPUT_PIG_FILE_PATH)
JAR_FILE_NAME=$(basename $DQ_GENERATOR_JAR_FILE_PATH)

# JSON Schema copy to hdfs
aws s3 cp $VALIDATION_SCHEMA_FOR_INPUT_JSON /var/tmp/
if [ $? -eq 0 ]
then
		echo "JSON schema file $SCHEMA_FILE_NAME for validation copied successfully"
else
		echo "copy of JSON schema file $SCHEMA_FILE_NAME for validation failed"
		exit 1
fi

# JSON copy to hdfs
aws s3 cp $INPUT_JSON_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
		echo "JSON input file $INPUT_JSON_FILE_NAME copied successfully"
else
		echo "copy of JSON input file $INPUT_JSON_FILE_NAME failed"
		exit 1
fi

# Copy JAR file to hdfs
aws s3 cp $DQ_GENERATOR_JAR_FILE_PATH /var/tmp/
if [ $? -eq 0 ]
then
		echo "JSON input file $JAR_FILE_NAME copied successfully"
else
		echo " copy of JSON input file $JAR_FILE_NAME failed"
		exit 1
fi

# Run java jar program for to generate DQ pig script
java -cp /var/tmp/EDH_JAVA-1.0-jar-with-dependencies.jar com.angieslist.edh.dq.dqgenerator.DQParser  /var/tmp/$INPUT_JSON_FILE_NAME /var/tmp/$SCHEMA_FILE_NAME

if [ $? -eq 0 ]
then
  		echo "$OUTPUT_PIG_FILE_NAME for performing DQ activity created successfully"
else
  		echo "$OUTPUT_PIG_FILE_NAME generation failed"
  		exit 1
fi

# Copy output pig file to s3
aws s3 cp $OUTPUT_PIG_FILE_NAME $OUTPUT_PIG_FILE_PATH
if [ $? -eq 0 ]
then
  		echo "$OUTPUT_PIG_FILE_NAME copied to s3"
else
  		echo "copying $OUTPUT_PIG_FILE_NAME to s3 failed"
  		exit 1
fi

# Pig Script to be triggered for data checking and cleansing.
pig \
	-param BUSDATE=$BUS_DATE \
	-param_file /var/tmp/$global_file \
	-param_file /var/tmp/$local_file \
	-file $OUTPUT_PIG_FILE_PATH \
	-useHCatalog

if [ $? -eq 0 ]
then
		echo "$OUTPUT_PIG_FILE_NAME executed without any error."
else
		echo "$OUTPUT_PIG_FILE_NAME execution failed."
		exit 1
fi

# Hive script to insert extraction audit record
hive -f $INCOMING_AUDIT_HQL_PATH \
		-hivevar ENTITY_NAME=$SOURCE_ALWEB \ 
		-hivevar INCOMING_DB=$ALWEB_INCOMING_DB \ 
		-hivevar INCOMING_TABLE=$TABLE_NAME_INC \ 
		-hivevar USER_NAME=$USER_NAME \ 
		-hivevar BUS_DATE=$BUS_DATE
		
# Hive Status check
if [ $? -eq 0 ]
then
		echo "$INCOMING_AUDIT_HQL_PATH executed without any error."
else
		echo "$INCOMING_AUDIT_HQL_PATH execution failed."
		exit 1
fi

# Hive script to insert DQ audit record
hive -f $DQ_AUDIT_HQL_PATH \
		-hivevar ENTITY_NAME=$SOURCE_ALWEB \ 
		-hivevar ALWEB_GOLD_DB=$ALWEB_GOLD_DB \ 
		-hivevar DQ_TABLE=$DQ_TABLE \ 
		-hivevar USER_NAME=$USER_NAME \ 
		-hivevar BUS_MONTH=$BUS_MONTH \
		-hivevar BUS_DATE=$BUS_DATE
		
# Hive Status check
if [ $? -eq 0 ]
then
		echo "$DQ_AUDIT_HQL_PATH executed without any error."
else
		echo "$DQ_AUDIT_HQL_PATH execution failed."
		exit 1
fi
